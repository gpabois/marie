use std::{
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use object_store::{
    ObjectStore, ObjectStoreExt as _,
    buffered::{BufReader, BufWriter},
    path::Path as ObjectPath,
};
use sqlx::{Row as _, postgres::PgPool};
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};
use super::{BoxedDescriptor, FileSystem, OpenOptions};
use crate::{
    id::IdGenerator,
    session::SessionId,
    workspace::WorkspaceId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InodeId(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InodeKind {
    Dir,
    File,
}

#[derive(Debug, Clone)]
pub struct Inode {
    pub id: InodeId,
    pub kind: InodeKind,
    /// Clé de l'objet dans l'`ObjectStore` porteur du contenu — uniquement
    /// pour `kind == File`.
    pub object_key: Option<String>,
    pub size: u64,
}

/// Arborescence de fichiers persistée en base — voir [`PostgresInodeCatalog`],
/// seule implémentation à ce jour. Chaque chemin résolu par une méthode de ce
/// trait est relatif à la racine du scope (voir
/// [`PostgresInodeCatalog::for_workspace`]/[`PostgresInodeCatalog::for_session`]) :
/// il n'y a pas de notion de chemin absolu traversant plusieurs scopes ici,
/// cette isolation est structurelle (voir `persistency::vfs::WorkspaceVfs`).
#[async_trait]
pub trait InodeCatalog: Send + Sync {
    /// L'inode à `path`, ou `None` s'il n'existe pas.
    async fn resolve(&self, path: &str) -> anyhow::Result<Option<Inode>>;

    /// Enfants directs du dossier `path`, `(nom, inode)`. Échoue si `path`
    /// n'existe pas (contrairement à [`Self::resolve`], qui renvoie `None`) :
    /// un appelant qui liste un chemin inconnu a presque toujours une erreur
    /// à se signaler, pas un dossier vide à afficher.
    async fn children(&self, path: &str) -> anyhow::Result<Vec<(String, Inode)>>;

    /// Crée `path` comme dossier, avec ses parents manquants (comme
    /// `mkdir -p`) — sans effet si `path` est déjà un dossier.
    async fn mkdir(&self, path: &str) -> anyhow::Result<Inode>;

    /// Crée (ou remplace) `path` comme fichier, avec ses parents manquants,
    /// et lui alloue une nouvelle clé d'objet — à charge de l'appelant
    /// d'écrire le contenu à cette clé dans l'`ObjectStore` associé (voir
    /// [`ObjectFileSystem::open`]).
    async fn create_file(&self, path: &str) -> anyhow::Result<Inode>;

    /// Supprime `path` — récursivement s'il s'agit d'un dossier — et renvoie
    /// les clés d'objet libérées (tous les inodes `kind == File` du
    /// sous-arbre supprimé), à charge de l'appelant de les effacer de
    /// l'`ObjectStore` associé (voir [`ObjectFileSystem::remove`]) : le
    /// catalogue ne connaît pas l'`ObjectStore`. Sans effet (et renvoie une
    /// liste vide) si `path` n'existe pas.
    async fn remove(&self, path: &str) -> anyhow::Result<Vec<String>>;
}

/// [`InodeCatalog`] adossé à PostgreSQL : une table `fs_inode` unique,
/// partagée par tous les scopes (voir [`Self::for_workspace`]/
/// [`Self::for_session`]) — chaque instance ne résout ses chemins qu'à
/// partir de sa propre racine (`root_id`), jamais celle d'un autre scope ni
/// la vraie racine de la table. Le sous-arbre d'une session est ainsi
/// littéralement un sous-arbre de celui de son workspace dans la même table
/// ("système d'inode récursif"), pas un espace de stockage séparé.
pub struct PostgresInodeCatalog {
    pool: PgPool,
    root_id: InodeId,
    ids: IdGenerator,
}

impl PostgresInodeCatalog {
    /// Racine dédiée à un workspace (`/workspaces/{workspace_id}`), créée au
    /// besoin sous la racine globale de la table (voir
    /// `persistency::postgres::run_migrations` pour le schéma de la table
    /// elle-même, à appliquer une fois par l'appelant avant toute
    /// utilisation de ce catalogue).
    pub async fn for_workspace(pool: PgPool, workspace_id: WorkspaceId) -> anyhow::Result<Self> {
        let global_root = ensure_global_root(&pool).await?;
        let root_id = ensure_path(&pool, global_root, &["workspaces", &workspace_id.to_string()]).await?;
        Ok(Self { pool, root_id, ids: IdGenerator::default() })
    }

    /// Racine dédiée à une session (`/workspaces/{workspace_id}/sessions/{session_id}`),
    /// imbriquée sous celle de son workspace — voir la doc de [`Self`].
    pub async fn for_session(pool: PgPool, workspace_id: WorkspaceId, session_id: SessionId) -> anyhow::Result<Self> {
        let global_root = ensure_global_root(&pool).await?;
        let root_id =
            ensure_path(&pool, global_root, &["workspaces", &workspace_id.to_string(), "sessions", &session_id.to_string()]).await?;
        Ok(Self { pool, root_id, ids: IdGenerator::default() })
    }

    /// Id de l'inode à `path`, relatif à `self.root_id` — `None` si un
    /// segment quelconque du chemin est introuvable.
    async fn walk(&self, path: &str) -> anyhow::Result<Option<InodeId>> {
        let mut current = self.root_id;
        for segment in split_path(path) {
            let row = sqlx::query("SELECT id FROM fs_inode WHERE parent_id = $1 AND name = $2")
                .bind(current.0)
                .bind(segment)
                .fetch_optional(&self.pool)
                .await?;
            let Some(row) = row else { return Ok(None) };
            current = InodeId(row.get("id"));
        }
        Ok(Some(current))
    }

    async fn load(&self, id: InodeId) -> anyhow::Result<Inode> {
        let row = sqlx::query("SELECT id, kind, object_key, size FROM fs_inode WHERE id = $1").bind(id.0).fetch_one(&self.pool).await?;
        Ok(row_to_inode(&row))
    }

    /// Id du dossier parent de `path` (créé au besoin, avec ses propres
    /// parents manquants) et nom du dernier segment — pour [`Self::mkdir`]/
    /// [`Self::create_file`].
    async fn resolve_parent(&self, path: &str) -> anyhow::Result<(InodeId, String)> {
        let segments = split_path(path);
        let Some((leaf, parents)) = segments.split_last() else {
            anyhow::bail!("chemin vide : la racine du scope existe déjà");
        };

        let mut current = self.root_id;
        for segment in parents {
            current = ensure_child_dir(&self.pool, current, segment).await?;
        }
        Ok((current, (*leaf).to_string()))
    }
}

#[async_trait]
impl InodeCatalog for PostgresInodeCatalog {
    async fn resolve(&self, path: &str) -> anyhow::Result<Option<Inode>> {
        match self.walk(path).await? {
            Some(id) => Ok(Some(self.load(id).await?)),
            None => Ok(None),
        }
    }

    async fn children(&self, path: &str) -> anyhow::Result<Vec<(String, Inode)>> {
        let Some(id) = self.walk(path).await? else {
            anyhow::bail!("dossier introuvable : {path}");
        };

        let rows = sqlx::query("SELECT name, id, kind, object_key, size FROM fs_inode WHERE parent_id = $1").bind(id.0).fetch_all(&self.pool).await?;
        Ok(rows.iter().map(|row| (row.get::<String, _>("name"), row_to_inode(row))).collect())
    }

    async fn mkdir(&self, path: &str) -> anyhow::Result<Inode> {
        let (parent, leaf) = self.resolve_parent(path).await?;
        let id = ensure_child_dir(&self.pool, parent, &leaf).await?;
        self.load(id).await
    }

    async fn create_file(&self, path: &str) -> anyhow::Result<Inode> {
        let (parent, leaf) = self.resolve_parent(path).await?;
        // Nouvelle clé à chaque (re)création plutôt que réutiliser un chemin
        // dérivé du nom : l'ancien objet, s'il y en avait un, devient
        // orphelin dans l'`ObjectStore` — acceptable pour l'instant (pas de
        // garbage collection ici), à traiter séparément si besoin.
        let object_key = format!("fs/{}", self.ids.next_id());

        let row = sqlx::query(
            "INSERT INTO fs_inode (parent_id, name, kind, object_key, size) VALUES ($1, $2, 1, $3, 0)
             ON CONFLICT (parent_id, name) DO UPDATE SET kind = 1, object_key = EXCLUDED.object_key, size = 0
             RETURNING id, kind, object_key, size",
        )
        .bind(parent.0)
        .bind(&leaf)
        .bind(&object_key)
        .fetch_one(&self.pool)
        .await?;

        Ok(row_to_inode(&row))
    }

    async fn remove(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let Some(id) = self.walk(path).await? else { return Ok(Vec::new()) };

        // Collecte les clés d'objet de tout le sous-arbre avant de le
        // supprimer — `ON DELETE CASCADE` sur `parent_id` s'occupe des
        // lignes, mais ne dit rien à l'`ObjectStore` associé.
        let rows = sqlx::query(
            "WITH RECURSIVE subtree AS (
                SELECT id, kind, object_key FROM fs_inode WHERE id = $1
                UNION ALL
                SELECT c.id, c.kind, c.object_key FROM fs_inode c JOIN subtree s ON c.parent_id = s.id
            )
            SELECT object_key FROM subtree WHERE kind = 1 AND object_key IS NOT NULL",
        )
        .bind(id.0)
        .fetch_all(&self.pool)
        .await?;
        let object_keys: Vec<String> = rows.iter().map(|row| row.get("object_key")).collect();

        sqlx::query("DELETE FROM fs_inode WHERE id = $1").bind(id.0).execute(&self.pool).await?;

        Ok(object_keys)
    }
}

fn split_path(path: &str) -> Vec<&str> {
    path.split('/').filter(|segment| !segment.is_empty()).collect()
}

fn row_to_inode(row: &sqlx::postgres::PgRow) -> Inode {
    let kind_code: i16 = row.get("kind");
    let kind = if kind_code == 1 { InodeKind::File } else { InodeKind::Dir };
    Inode { id: InodeId(row.get("id")), kind, object_key: row.get("object_key"), size: row.get::<i64, _>("size") as u64 }
}

async fn ensure_global_root(pool: &PgPool) -> anyhow::Result<InodeId> {
    if let Some(row) = sqlx::query("SELECT id FROM fs_inode WHERE parent_id IS NULL").fetch_optional(pool).await? {
        return Ok(InodeId(row.get("id")));
    }

    // Course possible entre le SELECT ci-dessus et cet INSERT si deux nœuds
    // démarrent en même temps : `fs_inode_root_idx` garantit qu'un seul
    // insert réussit, l'autre retombe sur le SELECT de secours ci-dessous.
    let inserted = sqlx::query("INSERT INTO fs_inode (parent_id, name, kind) VALUES (NULL, '', 0) ON CONFLICT ((1)) WHERE parent_id IS NULL DO NOTHING RETURNING id")
        .fetch_optional(pool)
        .await?;

    match inserted {
        Some(row) => Ok(InodeId(row.get("id"))),
        None => {
            let row = sqlx::query("SELECT id FROM fs_inode WHERE parent_id IS NULL").fetch_one(pool).await?;
            Ok(InodeId(row.get("id")))
        }
    }
}

/// Crée (au besoin) chaque segment de `segments` comme sous-dossier de
/// `root`, dans l'ordre — un chemin comme `workspaces/{id}/sessions/{id}`
/// est une chaîne de lignes `fs_inode` parent -> enfant, pas un préfixe de
/// clé plat (voir la doc de [`PostgresInodeCatalog`]).
async fn ensure_path(pool: &PgPool, root: InodeId, segments: &[&str]) -> anyhow::Result<InodeId> {
    let mut current = root;
    for segment in segments {
        current = ensure_child_dir(pool, current, segment).await?;
    }
    Ok(current)
}

async fn ensure_child_dir(pool: &PgPool, parent: InodeId, name: &str) -> anyhow::Result<InodeId> {
    let inserted = sqlx::query("INSERT INTO fs_inode (parent_id, name, kind) VALUES ($1, $2, 0) ON CONFLICT (parent_id, name) DO NOTHING RETURNING id")
        .bind(parent.0)
        .bind(name)
        .fetch_optional(pool)
        .await?;

    match inserted {
        Some(row) => Ok(InodeId(row.get("id"))),
        None => {
            let row = sqlx::query("SELECT id FROM fs_inode WHERE parent_id = $1 AND name = $2").bind(parent.0).bind(name).fetch_one(pool).await?;
            Ok(InodeId(row.get("id")))
        }
    }
}

/// [`FileSystem`] adossé à un [`InodeCatalog`] (arborescence) et un
/// `ObjectStore` (contenu) — voir `persistency::vfs::WorkspaceVfs` pour son
/// montage sur `/files` (portée workspace) et `/session/files` (portée
/// session, catalogue scopé différemment mais même implémentation).
pub struct ObjectFileSystem {
    catalog: Arc<dyn InodeCatalog>,
    store: Arc<dyn ObjectStore>,
}

impl ObjectFileSystem {
    pub fn new(catalog: Arc<dyn InodeCatalog>, store: Arc<dyn ObjectStore>) -> Self {
        Self { catalog, store }
    }
}

#[async_trait]
impl FileSystem for ObjectFileSystem {
    async fn mkdir(&self, path: &str) -> anyhow::Result<()> {
        self.catalog.mkdir(path).await?;
        Ok(())
    }

    async fn ls(&self, path: &str) -> anyhow::Result<Vec<String>> {
        Ok(self.catalog.children(path).await?.into_iter().map(|(name, _)| name).collect())
    }

    async fn open(&self, path: &str, options: OpenOptions) -> anyhow::Result<BoxedDescriptor> {
        let existing = self.catalog.resolve(path).await?;

        let inode = match existing {
            Some(inode) if matches!(inode.kind, InodeKind::File) => inode,
            Some(_) => anyhow::bail!("{path} est un dossier"),
            None if options.create || options.create_new => self.catalog.create_file(path).await?,
            None => anyhow::bail!("fichier introuvable : {path}"),
        };

        let object_key = ObjectPath::from(inode.object_key.expect("un inode de kind File porte toujours un object_key"));

        // `object_store` ne permet pas la lecture-écriture simultanée d'un
        // même objet (contrairement à un vrai descripteur POSIX) : le sens
        // demandé par `options` détermine lequel des deux est ouvert (voir
        // [`ObjectDescriptor`]).
        if options.write || options.append || options.truncate {
            let writer = BufWriter::new(self.store.clone(), object_key);
            Ok(Box::pin(ObjectDescriptor::Write(writer)))
        } else {
            let meta = self.store.head(&object_key).await?;
            let reader = BufReader::new(self.store.clone(), &meta);
            Ok(Box::pin(ObjectDescriptor::Read(reader)))
        }
    }

    async fn remove(&self, path: &str) -> anyhow::Result<()> {
        let object_keys = self.catalog.remove(path).await?;
        for object_key in object_keys {
            self.store.delete(&ObjectPath::from(object_key)).await?;
        }
        Ok(())
    }
}

/// Descripteur retourné par [`ObjectFileSystem::open`] : n'expose que le
/// sens (lecture ou écriture) demandé à l'ouverture — l'autre renvoie une
/// erreur E/S plutôt que de bloquer ou de paniquer (voir la note dans
/// [`ObjectFileSystem::open`]).
enum ObjectDescriptor {
    Read(BufReader),
    Write(BufWriter),
}

fn unsupported(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Unsupported, message.to_string())
}

impl AsyncRead for ObjectDescriptor {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ObjectDescriptor::Read(reader) => Pin::new(reader).poll_read(cx, buf),
            ObjectDescriptor::Write(_) => Poll::Ready(Err(unsupported("lecture non supportée sur un descripteur ouvert en écriture"))),
        }
    }
}

impl AsyncWrite for ObjectDescriptor {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            ObjectDescriptor::Write(writer) => Pin::new(writer).poll_write(cx, buf),
            ObjectDescriptor::Read(_) => Poll::Ready(Err(unsupported("écriture non supportée sur un descripteur ouvert en lecture"))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ObjectDescriptor::Write(writer) => Pin::new(writer).poll_flush(cx),
            ObjectDescriptor::Read(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            ObjectDescriptor::Write(writer) => Pin::new(writer).poll_shutdown(cx),
            ObjectDescriptor::Read(_) => Poll::Ready(Ok(())),
        }
    }
}

impl AsyncSeek for ObjectDescriptor {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        match self.get_mut() {
            ObjectDescriptor::Read(reader) => Pin::new(reader).start_seek(position),
            ObjectDescriptor::Write(_) => Err(unsupported("seek non supporté sur un descripteur ouvert en écriture")),
        }
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        match self.get_mut() {
            ObjectDescriptor::Read(reader) => Pin::new(reader).poll_complete(cx),
            ObjectDescriptor::Write(_) => Poll::Ready(Err(unsupported("seek non supporté sur un descripteur ouvert en écriture"))),
        }
    }
}
