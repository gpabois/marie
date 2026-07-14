use std::{pin::Pin, sync::Arc};

use async_trait::async_trait;
use object_store::{ObjectStore, aws::AmazonS3Builder, memory::InMemory};
use tokio::{io::{AsyncRead, AsyncSeek, AsyncWrite}, sync::RwLock};
use typed_builder::TypedBuilder;

use crate::persistency::alias::AliasCatalog;

#[derive(TypedBuilder, Clone, Copy, Debug, Default)]
pub struct OpenOptions {
    #[builder(default)]
    pub read: bool,
    #[builder(default)]
    pub write: bool,
    #[builder(default)]
    pub append: bool,
    #[builder(default)]
    pub truncate: bool,
    #[builder(default)]
    pub create: bool,
    #[builder(default)]
    pub create_new: bool,
}

/// N'importe quel descripteur de fichier ouvert par [`FileSystem::open`] —
/// implémenté automatiquement par tout type lisant/écrivant/cherchant de
/// façon asynchrone (`object_store::buffered::BufReader`/`BufWriter`, un
/// simple `Cursor` mémoire, ...), sans avoir à le déclarer explicitement.
pub trait AsyncFile: AsyncRead + AsyncWrite + AsyncSeek + Send {}
impl<T: AsyncRead + AsyncWrite + AsyncSeek + Send> AsyncFile for T {}

/// Descripteur retourné par [`FileSystem::open`], boîté et unifié entre tous
/// les backends — contrairement à un type associé générique (ce qui rendrait
/// [`FileSystem`] non dyn-compatible), un seul type concret ici permet à
/// [`VFS`] de router entre des systèmes de fichiers hétérogènes (`/var`
/// adossé à un CRDT, `/files` à un `ObjectStore`, ...) derrière un unique
/// `Arc<dyn FileSystem>` par montage.
pub type BoxedDescriptor = Pin<Box<dyn AsyncFile>>;

/// Système de fichiers minimal (mkdir/ls/open), voir [`BoxedDescriptor`] pour
/// pourquoi il ne porte pas de type associé. `mount`/`alias` n'en font
/// délibérément pas partie : seul le routeur [`VFS`] en a besoin, pas les
/// systèmes de fichiers concrets qu'il monte (voir `persistency::var::VarFileSystem`,
/// `persistency::inode::ObjectFileSystem`).
#[async_trait]
pub trait FileSystem: Send + Sync {
    async fn mkdir(&self, path: &str) -> anyhow::Result<()>;
    async fn ls(&self, path: &str) -> anyhow::Result<Vec<String>>;
    async fn open(&self, path: &str, options: OpenOptions) -> anyhow::Result<BoxedDescriptor>;
    /// Supprime `path` — récursivement s'il s'agit d'un dossier. Sans effet
    /// s'il n'existe pas.
    async fn remove(&self, path: &str) -> anyhow::Result<()>;
}

/// Nombre maximal de réécritures suivies lors de la résolution d'un alias
/// (voir [`VFS::resolve_aliases`]) — garde-fou contre un cycle (`a -> b`,
/// `b -> a`), jamais atteint par un usage normal (les alias visés ici sont
/// des raccourcis courts, pas des chaînes profondes).
const MAX_ALIAS_HOPS: u8 = 8;

/// Routeur de systèmes de fichiers : associe des préfixes de chemin absolus
/// (ex: `/var`, `/files`, `/session`) à un [`FileSystem`] concret, et
/// délègue chaque opération au montage dont le préfixe est le plus long à
/// matcher `path` — même principe qu'un `mount` Unix. Implémente lui-même
/// [`FileSystem`], donc composable : un `VFS` peut être monté à l'intérieur
/// d'un autre (voir `persistency::vfs::WorkspaceVfs::mount_session`, qui
/// monte le VFS d'une session sous `/session` dans celui de son workspace).
///
/// Porte optionnellement une [`AliasCatalog`] (voir [`Self::with_aliases`]),
/// consultée avant le routage de montage : un alias se comporte comme un
/// lien symbolique Unix sur un dossier (`/current -> /session/files` fait que
/// `/current/rapport.md` se résout comme `/session/files/rapport.md`). Un VFS
/// imbriqué (ex: celui d'une session) porte sa propre table, distincte de
/// celle du VFS englobant (celui du workspace) — la résolution suit donc
/// naturellement la même hiérarchie que les montages, sans code dédié.
#[derive(Default)]
pub struct VFS {
    mounts: RwLock<Vec<(String, Arc<dyn FileSystem>)>>,
    aliases: Option<Arc<dyn AliasCatalog>>,
}

impl VFS {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_aliases(aliases: Arc<dyn AliasCatalog>) -> Self {
        Self { mounts: RwLock::new(Vec::new()), aliases: Some(aliases) }
    }

    /// Monte `fs` sur `prefix` (ex: `/var`) — un montage déjà présent sur ce
    /// préfixe exact est remplacé. Garde les montages triés du préfixe le
    /// plus long au plus court, pour que `/session/var` matche avant
    /// `/session` en résolution.
    pub async fn mount(&self, prefix: &str, fs: Arc<dyn FileSystem>) {
        let mut mounts = self.mounts.write().await;
        mounts.retain(|(existing, _)| existing != prefix);
        mounts.push((prefix.to_string(), fs));
        mounts.sort_by(|(a, _), (b, _)| b.len().cmp(&a.len()));
    }

    /// Enregistre un alias `from -> to` — sans effet possible si ce VFS n'a
    /// pas de table d'alias (voir [`Self::with_aliases`]).
    pub async fn alias(&self, from: &str, to: &str) -> anyhow::Result<()> {
        let Some(aliases) = &self.aliases else {
            anyhow::bail!("ce VFS n'a pas de table d'alias");
        };
        aliases.set(from, to).await
    }

    /// Réécrit `path` en suivant les alias enregistrés (voir la doc de
    /// [`Self`]), jusqu'à [`MAX_ALIAS_HOPS`] réécritures.
    async fn resolve_aliases(&self, path: &str) -> anyhow::Result<String> {
        let mut current = path.to_string();
        if let Some(aliases) = &self.aliases {
            for _ in 0..MAX_ALIAS_HOPS {
                match aliases.resolve_prefix(&current).await? {
                    Some(rewritten) if rewritten != current => current = rewritten,
                    _ => break,
                }
            }
        }
        Ok(current)
    }

    /// Montage dont le préfixe est le plus long à matcher `path`, avec le
    /// sous-chemin relatif à ce montage (toujours préfixé de `/`).
    async fn resolve_mount(&self, path: &str) -> Option<(Arc<dyn FileSystem>, String)> {
        let mounts = self.mounts.read().await;
        for (prefix, fs) in mounts.iter() {
            let Some(rest) = path.strip_prefix(prefix.as_str()) else { continue };
            if rest.is_empty() || rest.starts_with('/') {
                let sub = if rest.is_empty() { "/".to_string() } else { rest.to_string() };
                return Some((fs.clone(), sub));
            }
        }
        None
    }

    async fn resolve(&self, path: &str) -> anyhow::Result<(Arc<dyn FileSystem>, String)> {
        let path = self.resolve_aliases(path).await?;
        self.resolve_mount(&path).await.ok_or_else(|| anyhow::anyhow!("aucun montage ne couvre {path}"))
    }
}

#[async_trait]
impl FileSystem for VFS {
    async fn mkdir(&self, path: &str) -> anyhow::Result<()> {
        let (fs, sub) = self.resolve(path).await?;
        fs.mkdir(&sub).await
    }

    async fn ls(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let (fs, sub) = self.resolve(path).await?;
        fs.ls(&sub).await
    }

    async fn open(&self, path: &str, options: OpenOptions) -> anyhow::Result<BoxedDescriptor> {
        let (fs, sub) = self.resolve(path).await?;
        fs.open(&sub, options).await
    }

    async fn remove(&self, path: &str) -> anyhow::Result<()> {
        let (fs, sub) = self.resolve(path).await?;
        fs.remove(&sub).await
    }
}

#[cfg(test)]
mod vfs_tests {
    use tokio::sync::Mutex as AsyncMutex;

    use super::*;

    /// [`FileSystem`] de test qui se contente d'enregistrer le sous-chemin
    /// reçu — vérifie ce que [`VFS`] lui a transmis, sans backend réel.
    struct RecordingFileSystem {
        name: &'static str,
        received: Arc<AsyncMutex<Vec<String>>>,
    }

    #[async_trait]
    impl FileSystem for RecordingFileSystem {
        async fn mkdir(&self, _path: &str) -> anyhow::Result<()> {
            Ok(())
        }

        async fn ls(&self, path: &str) -> anyhow::Result<Vec<String>> {
            self.received.lock().await.push(path.to_string());
            Ok(vec![self.name.to_string()])
        }

        async fn open(&self, _path: &str, _options: OpenOptions) -> anyhow::Result<BoxedDescriptor> {
            anyhow::bail!("non utilisé dans ces tests")
        }

        async fn remove(&self, _path: &str) -> anyhow::Result<()> {
            anyhow::bail!("non utilisé dans ces tests")
        }
    }

    #[tokio::test]
    async fn test_resolve_picks_longest_matching_prefix() {
        let vfs = VFS::new();
        let session_received = Arc::new(AsyncMutex::new(Vec::new()));
        let session_var_received = Arc::new(AsyncMutex::new(Vec::new()));

        vfs.mount("/session", Arc::new(RecordingFileSystem { name: "session", received: session_received.clone() })).await;
        vfs.mount("/session/var", Arc::new(RecordingFileSystem { name: "session-var", received: session_var_received.clone() })).await;

        let result = vfs.ls("/session/var/foo").await.unwrap();
        assert_eq!(result, vec!["session-var".to_string()]);
        assert_eq!(session_var_received.lock().await.as_slice(), ["/foo".to_string()]);
        assert!(session_received.lock().await.is_empty());
    }

    #[tokio::test]
    async fn test_resolve_falls_back_to_shorter_prefix_and_strips_it() {
        let vfs = VFS::new();
        let received = Arc::new(AsyncMutex::new(Vec::new()));
        vfs.mount("/session", Arc::new(RecordingFileSystem { name: "session", received: received.clone() })).await;

        vfs.ls("/session/files/report.md").await.unwrap();
        assert_eq!(received.lock().await.as_slice(), ["/files/report.md".to_string()]);
    }

    #[tokio::test]
    async fn test_ls_on_unmounted_path_fails() {
        let vfs = VFS::new();
        assert!(vfs.ls("/nowhere").await.is_err());
    }

    #[derive(Default)]
    struct MemoryAliasCatalog(AsyncMutex<Vec<(String, String)>>);

    #[async_trait]
    impl AliasCatalog for MemoryAliasCatalog {
        async fn resolve_prefix(&self, path: &str) -> anyhow::Result<Option<String>> {
            let aliases = self.0.lock().await;
            let best =
                aliases.iter().filter(|(from, _)| path == from.as_str() || path.starts_with(&format!("{from}/"))).max_by_key(|(from, _)| from.len());
            Ok(best.map(|(from, to)| format!("{to}{}", &path[from.len()..])))
        }

        async fn set(&self, from: &str, to: &str) -> anyhow::Result<()> {
            self.0.lock().await.push((from.to_string(), to.to_string()));
            Ok(())
        }

        async fn remove(&self, from: &str) -> anyhow::Result<()> {
            self.0.lock().await.retain(|(f, _)| f != from);
            Ok(())
        }

        async fn list(&self) -> anyhow::Result<Vec<(String, String)>> {
            Ok(self.0.lock().await.clone())
        }
    }

    #[tokio::test]
    async fn test_alias_rewrites_path_before_mount_matching() {
        let vfs = VFS::with_aliases(Arc::new(MemoryAliasCatalog::default()));
        let received = Arc::new(AsyncMutex::new(Vec::new()));
        vfs.mount("/session/files", Arc::new(RecordingFileSystem { name: "files", received: received.clone() })).await;
        vfs.alias("/current", "/session/files").await.unwrap();

        vfs.ls("/current/report.md").await.unwrap();
        assert_eq!(received.lock().await.as_slice(), ["/report.md".to_string()]);
    }

    #[tokio::test]
    async fn test_alias_without_catalog_errors_on_set() {
        let vfs = VFS::new();
        assert!(vfs.alias("/current", "/session").await.is_err());
    }
}

/// Backend de stockage objet du VFS (voir `persistency::vfs::WorkspaceVfs`,
/// `persistency::inode::ObjectFileSystem`) : choisi indépendamment du
/// contenu qu'il stocke — la mémoire pour les déploiements sans besoin de
/// durabilité (tests, cluster jetable), un bucket S3 ou compatible S3
/// (MinIO, etc.) pour le reste. D'autres backends `object_store` (GCS,
/// Azure, système de fichiers local) peuvent s'ajouter ici sans rien changer
/// côté appelant.
pub enum FilesystemConfig {
    /// Rien n'est conservé après l'arrêt du processus.
    Memory,
    S3 {
        bucket: String,
        region: String,
        access_key_id: String,
        secret_access_key: String,
        /// `None` pour AWS S3 ; renseigné pour un provider compatible S3
        /// auto-hébergé (ex. `http://localhost:9000` pour MinIO).
        endpoint: Option<String>,
    },
}

impl FilesystemConfig {
    pub fn build(&self) -> anyhow::Result<Arc<dyn ObjectStore>> {
        match self {
            Self::Memory => Ok(Arc::new(InMemory::new())),
            Self::S3 { bucket, region, access_key_id, secret_access_key, endpoint } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_region(region)
                    .with_access_key_id(access_key_id)
                    .with_secret_access_key(secret_access_key);

                // Un provider compatible S3 auto-hébergé n'est en général pas
                // adressable par sous-domaine de bucket (style hébergé
                // virtuellement, le défaut AWS) : MinIO et consorts exigent le
                // style par chemin (`endpoint/bucket/clé`).
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_virtual_hosted_style_request(false)
                        .with_allow_http(endpoint.starts_with("http://"));
                }

                Ok(Arc::new(builder.build()?))
            }
        }
    }
}
