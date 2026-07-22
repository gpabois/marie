use std::collections::BTreeSet;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::stream::{self, BoxStream, StreamExt};
use object_store::{
    Attributes, CopyMode, CopyOptions, Error as StoreError, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart, path::Path,
};
use sqlx::{
    Row as _,
    postgres::{PgPool, PgRow},
};

/// [`ObjectStore`] adossé à PostgreSQL — une ligne par objet dans la table
/// `fs_object` (chemin complet en clé primaire), schéma géré par les
/// migrations (voir `store::postgres::run_migrations`, à appeler une fois par
/// l'appelant avant toute utilisation). Alternative à `InMemory`/`AmazonS3`
/// (voir [`super::FilesystemConfig`]) pour les déploiements qui préfèrent
/// garder le contenu des fichiers dans la même base que le reste de l'état du
/// cluster plutôt que de dépendre d'un bucket S3/MinIO séparé — pertinent
/// surtout pour de petits volumes, `fs_object.data` n'étant qu'une colonne
/// `BYTEA` sans découpage en chunks côté stockage.
#[derive(Clone)]
pub struct PostgresObjectStore(PgPool);

impl PostgresObjectStore {
    pub fn new(pool: PgPool) -> Self {
        Self(pool)
    }
}

impl std::fmt::Debug for PostgresObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PostgresObjectStore")
    }
}

impl std::fmt::Display for PostgresObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PostgresObjectStore")
    }
}

#[async_trait]
impl ObjectStore for PostgresObjectStore {
    async fn put_opts(&self, location: &Path, payload: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let data: Bytes = payload.into();
        let size = data.len() as i64;
        let path = location.as_ref();

        let row = match opts.mode {
            PutMode::Overwrite => sqlx::query(
                "INSERT INTO fs_object (path, data, size, e_tag, last_modified)
                 VALUES ($1, $2, $3, nextval('fs_object_etag_seq'), now())
                 ON CONFLICT (path) DO UPDATE
                     SET data = EXCLUDED.data, size = EXCLUDED.size,
                         e_tag = nextval('fs_object_etag_seq'), last_modified = now()
                 RETURNING e_tag, last_modified",
            )
            .bind(path)
            .bind(data.as_ref())
            .bind(size)
            .fetch_one(&self.0)
            .await
            .map_err(postgres_error)?,

            PutMode::Create => sqlx::query(
                "INSERT INTO fs_object (path, data, size, e_tag, last_modified)
                 VALUES ($1, $2, $3, nextval('fs_object_etag_seq'), now())
                 ON CONFLICT (path) DO NOTHING
                 RETURNING e_tag, last_modified",
            )
            .bind(path)
            .bind(data.as_ref())
            .bind(size)
            .fetch_optional(&self.0)
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| StoreError::AlreadyExists { path: path.to_string(), source: format!("un objet existe déjà à {path}").into() })?,

            PutMode::Update(version) => {
                // Comme l'implémentation mémoire de référence : un ETag manquant
                // ou ne correspondant plus à la version actuelle sont tous deux
                // traités comme une précondition non satisfaite, pas une erreur
                // distincte — l'appelant n'a de toute façon qu'à relire l'objet
                // et retenter.
                let expected: i64 = version
                    .e_tag
                    .as_deref()
                    .and_then(|e_tag| e_tag.parse::<i64>().ok())
                    .ok_or_else(|| StoreError::Generic { store: "Postgres", source: "un ETag est requis pour une mise à jour conditionnelle".into() })?;

                sqlx::query(
                    "UPDATE fs_object
                     SET data = $2, size = $3, e_tag = nextval('fs_object_etag_seq'), last_modified = now()
                     WHERE path = $1 AND e_tag = $4
                     RETURNING e_tag, last_modified",
                )
                .bind(path)
                .bind(data.as_ref())
                .bind(size)
                .bind(expected)
                .fetch_optional(&self.0)
                .await
                .map_err(postgres_error)?
                .ok_or_else(|| StoreError::Precondition {
                    path: path.to_string(),
                    source: "l'ETag ne correspond plus à la version actuelle (ou l'objet n'existe pas)".into(),
                })?
            }
        };

        Ok(PutResult { e_tag: Some(row.get::<i64, _>("e_tag").to_string()), version: None, extensions: Default::default() })
    }

    async fn put_multipart_opts(&self, location: &Path, _opts: PutMultipartOptions) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(PostgresMultipartUpload { pool: self.0.clone(), location: location.clone(), parts: Vec::new() }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let path = location.as_ref();

        let row = sqlx::query("SELECT data, size, e_tag, last_modified FROM fs_object WHERE path = $1")
            .bind(path)
            .fetch_optional(&self.0)
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| StoreError::NotFound { path: path.to_string(), source: "objet introuvable".into() })?;

        let data = Bytes::from(row.get::<Vec<u8>, _>("data"));
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: row.get::<DateTime<Utc>, _>("last_modified"),
            size: row.get::<i64, _>("size") as u64,
            e_tag: Some(row.get::<i64, _>("e_tag").to_string()),
            version: None,
        };
        options.check_preconditions(&meta)?;

        let (range, bytes) = match &options.range {
            Some(range) => {
                let r = range.as_range(data.len() as u64).map_err(|source| StoreError::Generic { store: "Postgres", source: Box::new(source) })?;
                (r.clone(), data.slice(r.start as usize..r.end as usize))
            }
            None => (0..data.len() as u64, data),
        };

        let stream = stream::once(futures::future::ready(Ok(bytes)));
        Ok(GetResult { payload: GetResultPayload::Stream(stream.boxed()), attributes: Attributes::default(), meta, range, extensions: Default::default() })
    }

    fn delete_stream(&self, locations: BoxStream<'static, Result<Path>>) -> BoxStream<'static, Result<Path>> {
        let pool = self.0.clone();
        locations
            .map(move |location| {
                let pool = pool.clone();
                async move {
                    let location = location?;
                    sqlx::query("DELETE FROM fs_object WHERE path = $1").bind(location.as_ref()).execute(&pool).await.map_err(postgres_error)?;
                    Ok(location)
                }
            })
            .buffered(10)
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let pool = self.0.clone();
        let prefix = prefix.cloned();
        stream::once(async move { fetch_objects(&pool, prefix.as_ref()).await })
            .flat_map(|result| match result {
                Ok(metas) => stream::iter(metas.into_iter().map(Ok)).boxed(),
                Err(err) => stream::once(async move { Err(err) }).boxed(),
            })
            .boxed()
    }

    /// Comme l'implémentation mémoire de référence, renvoie tout en un seul
    /// appel plutôt que de paginer — acceptable ici pour le même volume
    /// modeste (fichiers d'une session/d'un workspace), pas un bucket public.
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let root = Path::default();
        let prefix = prefix.unwrap_or(&root);
        let metas = fetch_objects(&self.0, Some(prefix)).await?;

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        for meta in metas {
            let is_nested = {
                let Some(mut parts) = meta.location.prefix_match(prefix) else { continue };
                let Some(common_prefix) = parts.next() else { continue };

                if parts.next().is_some() {
                    common_prefixes.insert(prefix.clone().join(common_prefix.as_ref().to_string()));
                    true
                } else {
                    false
                }
            };

            if !is_nested {
                objects.push(meta);
            }
        }

        Ok(ListResult { common_prefixes: common_prefixes.into_iter().collect(), objects, extensions: Default::default() })
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let row = sqlx::query("SELECT data FROM fs_object WHERE path = $1")
            .bind(from.as_ref())
            .fetch_optional(&self.0)
            .await
            .map_err(postgres_error)?
            .ok_or_else(|| StoreError::NotFound { path: from.to_string(), source: "objet source introuvable".into() })?;

        let data: Vec<u8> = row.get("data");
        let size = data.len() as i64;

        match options.mode {
            CopyMode::Overwrite => {
                sqlx::query(
                    "INSERT INTO fs_object (path, data, size, e_tag, last_modified)
                     VALUES ($1, $2, $3, nextval('fs_object_etag_seq'), now())
                     ON CONFLICT (path) DO UPDATE
                         SET data = EXCLUDED.data, size = EXCLUDED.size,
                             e_tag = nextval('fs_object_etag_seq'), last_modified = now()",
                )
                .bind(to.as_ref())
                .bind(&data)
                .bind(size)
                .execute(&self.0)
                .await
                .map_err(postgres_error)?;
            }
            CopyMode::Create => {
                let result = sqlx::query(
                    "INSERT INTO fs_object (path, data, size, e_tag, last_modified)
                     VALUES ($1, $2, $3, nextval('fs_object_etag_seq'), now())
                     ON CONFLICT (path) DO NOTHING",
                )
                .bind(to.as_ref())
                .bind(&data)
                .bind(size)
                .execute(&self.0)
                .await
                .map_err(postgres_error)?;

                if result.rows_affected() == 0 {
                    return Err(StoreError::AlreadyExists { path: to.to_string(), source: format!("un objet existe déjà à {to}").into() });
                }
            }
        }

        Ok(())
    }
}

/// Upload multipart bufferisé en mémoire le temps de la session d'upload —
/// contrairement à S3/GCS, `fs_object.data` n'a pas de notion de parts côté
/// stockage : chaque partie est simplement accumulée jusqu'à
/// [`MultipartUpload::complete`], qui les concatène et fait un `put_opts`
/// unique (`PutMode::Overwrite`).
#[derive(Debug)]
struct PostgresMultipartUpload {
    pool: PgPool,
    location: Path,
    parts: Vec<PutPayload>,
}

#[async_trait]
impl MultipartUpload for PostgresMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.parts.push(data);
        Box::pin(futures::future::ready(Ok(())))
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let capacity = self.parts.iter().map(PutPayload::content_length).sum();
        let mut buf = Vec::with_capacity(capacity);
        for part in &self.parts {
            for chunk in part {
                buf.extend_from_slice(chunk);
            }
        }

        let store = PostgresObjectStore(self.pool.clone());
        store.put_opts(&self.location, buf.into(), PutOptions::default()).await
    }

    async fn abort(&mut self) -> Result<()> {
        self.parts.clear();
        Ok(())
    }
}

fn postgres_error(source: sqlx::Error) -> StoreError {
    StoreError::Generic { store: "Postgres", source: Box::new(source) }
}

fn row_to_meta(row: &PgRow) -> ObjectMeta {
    ObjectMeta {
        location: Path::from(row.get::<String, _>("path")),
        last_modified: row.get::<DateTime<Utc>, _>("last_modified"),
        size: row.get::<i64, _>("size") as u64,
        e_tag: Some(row.get::<i64, _>("e_tag").to_string()),
        version: None,
    }
}

/// Échappe `%`/`_` (jokers `LIKE`) et `\` (caractère d'échappement lui-même)
/// dans un segment de chemin utilisé comme motif `LIKE ... ESCAPE '\'` —
/// nécessaire car un chemin peut légitimement contenir n'importe lequel de
/// ces caractères sans intention de filtrage.
fn escape_like(segment: &str) -> String {
    segment.replace('\\', "\\\\").replace('%', "\\%").replace('_', "\\_")
}

/// Tous les objets sous `prefix` (récursif, mêmes bornes de segment qu'un
/// [`Path`] — `foo/bar` matche `foo/bar/x` mais pas `foo/bar_baz/x`), ou tous
/// les objets si `prefix` est `None`/racine. Poussé en `LIKE` côté SQL plutôt
/// que filtré après coup en Rust (comme le fait l'implémentation mémoire de
/// référence sur sa `BTreeMap`) : la frontière `/` explicite dans le motif
/// suffit à garantir la même sémantique par segment sans réimplémenter
/// `Path::prefix_match` ici.
async fn fetch_objects(pool: &PgPool, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>> {
    let rows = match prefix.filter(|prefix| prefix.parts_count() > 0) {
        Some(prefix) => {
            let pattern = format!("{}/%", escape_like(prefix.as_ref()));
            sqlx::query("SELECT path, size, e_tag, last_modified FROM fs_object WHERE path LIKE $1 ESCAPE '\\' ORDER BY path")
                .bind(pattern)
                .fetch_all(pool)
                .await
        }
        None => sqlx::query("SELECT path, size, e_tag, last_modified FROM fs_object ORDER BY path").fetch_all(pool).await,
    }
    .map_err(postgres_error)?;

    Ok(rows.iter().map(row_to_meta).collect())
}
