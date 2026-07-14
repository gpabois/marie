use std::{
    collections::HashMap,
    future::Future,
    io::SeekFrom,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use async_trait::async_trait;
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncSeek, AsyncWrite, ReadBuf};

use crate::{
    persistency::filesystem::{BoxedDescriptor, FileSystem, OpenOptions},
    session::{SessionId, client::SessionClient},
    workspace::{WorkspaceId, client::WorkspaceClient},
};

/// Store clé-valeur plat, backend de `/var` (portée workspace) et
/// `/session/var` (portée session) dans le VFS — pas de stockage dédié : les
/// deux implémentations ([`WorkspaceVarStore`], [`SessionVarStore`]) délèguent
/// au CRDT `yrs` déjà porté par `workspace::crdt::YrsWorkspace::state` /
/// `session::crdt::YrsSession::state`, gossipé et répliqué comme le reste de
/// la session/du workspace.
#[async_trait]
pub trait VarStore: Send + Sync {
    async fn value(&self, key: &str) -> Option<Value>;
    async fn set_value(&self, key: &str, value: Value) -> anyhow::Result<()>;
    async fn remove_value(&self, key: &str) -> anyhow::Result<()>;
    async fn values(&self) -> HashMap<String, Value>;
}

/// [`VarStore`] adossé au store clé-valeur d'un workspace (voir
/// [`WorkspaceClient`]) — un workspace à la fois : `WorkspaceClient` en gère
/// potentiellement plusieurs, mais un [`crate::persistency::filesystem::VFS`]
/// n'en monte qu'un seul sur `/var`.
pub struct WorkspaceVarStore {
    client: WorkspaceClient,
    workspace_id: WorkspaceId,
}

impl WorkspaceVarStore {
    pub fn new(client: WorkspaceClient, workspace_id: WorkspaceId) -> Self {
        Self { client, workspace_id }
    }
}

#[async_trait]
impl VarStore for WorkspaceVarStore {
    async fn value(&self, key: &str) -> Option<Value> {
        self.client.value(self.workspace_id, key).await
    }

    async fn set_value(&self, key: &str, value: Value) -> anyhow::Result<()> {
        self.client.set_value(self.workspace_id, key.to_string(), value).await
    }

    async fn remove_value(&self, key: &str) -> anyhow::Result<()> {
        self.client.remove_value(self.workspace_id, key.to_string()).await
    }

    async fn values(&self) -> HashMap<String, Value> {
        self.client.values(self.workspace_id).await
    }
}

/// [`VarStore`] adossé au store clé-valeur d'une session (voir
/// [`SessionClient`]) — même principe que [`WorkspaceVarStore`].
pub struct SessionVarStore {
    client: SessionClient,
    session_id: SessionId,
}

impl SessionVarStore {
    pub fn new(client: SessionClient, session_id: SessionId) -> Self {
        Self { client, session_id }
    }
}

#[async_trait]
impl VarStore for SessionVarStore {
    async fn value(&self, key: &str) -> Option<Value> {
        self.client.value(self.session_id, key).await
    }

    async fn set_value(&self, key: &str, value: Value) -> anyhow::Result<()> {
        self.client.set_value(self.session_id, key.to_string(), value).await
    }

    async fn remove_value(&self, key: &str) -> anyhow::Result<()> {
        self.client.remove_value(self.session_id, key.to_string()).await
    }

    async fn values(&self) -> HashMap<String, Value> {
        self.client.values(self.session_id).await
    }
}

/// [`FileSystem`] adossé à un [`VarStore`] : un chemin `/foo/bar` correspond
/// à la clé plate `foo.bar` (segments joints par `.`), pas à une arborescence
/// imbriquée — cohérent avec `WorkspaceApi`/`SessionApi::values`, qui
/// exposent déjà un `HashMap<String, Value>` plat.
pub struct VarFileSystem {
    store: Arc<dyn VarStore>,
}

impl VarFileSystem {
    pub fn new(store: Arc<dyn VarStore>) -> Self {
        Self { store }
    }
}

fn path_to_key(path: &str) -> String {
    path.split('/').filter(|segment| !segment.is_empty()).collect::<Vec<_>>().join(".")
}

/// Si `key` est `prefix` suivi d'au moins un segment, le premier segment
/// suivant — réduit un `values()` plat au premier niveau, comme des entrées
/// de dossier pour [`VarFileSystem::ls`].
fn child_name(prefix: &str, key: &str) -> Option<String> {
    let rest = if prefix.is_empty() { Some(key) } else { key.strip_prefix(prefix).and_then(|rest| rest.strip_prefix('.')) };
    let rest = rest?;
    if rest.is_empty() {
        return None;
    }
    Some(rest.split('.').next().unwrap_or(rest).to_string())
}

/// Décode le contenu écrit dans un descripteur `/var` en [`Value`] : un texte
/// JSON valide (`1`, `"texte"`, `{"a":1}`, ...) est interprété comme tel,
/// sinon le texte brut devient une chaîne — pour qu'écrire `1` dans un
/// fichier stocke bien le nombre `1`, pas la chaîne `"1"`.
fn decode_value(bytes: &[u8]) -> Value {
    let text = String::from_utf8_lossy(bytes);
    serde_json::from_str(text.trim()).unwrap_or_else(|_| Value::String(text.into_owned()))
}

/// Encode une [`Value`] en octets lisibles : une chaîne s'écrit telle quelle
/// (sans guillemets JSON), tout le reste en JSON — symétrique de
/// [`decode_value`].
fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::String(text) => text.clone().into_bytes(),
        other => serde_json::to_vec(other).unwrap_or_default(),
    }
}

#[async_trait]
impl FileSystem for VarFileSystem {
    async fn mkdir(&self, path: &str) -> anyhow::Result<()> {
        let key = path_to_key(path);
        if self.store.value(&key).await.is_none() {
            self.store.set_value(&key, Value::Object(serde_json::Map::new())).await?;
        }
        Ok(())
    }

    async fn ls(&self, path: &str) -> anyhow::Result<Vec<String>> {
        let prefix = path_to_key(path);
        let values = self.store.values().await;

        let mut children: Vec<String> = values.keys().filter_map(|key| child_name(&prefix, key)).collect();
        children.sort();
        children.dedup();
        Ok(children)
    }

    async fn open(&self, path: &str, options: OpenOptions) -> anyhow::Result<BoxedDescriptor> {
        let key = path_to_key(path);
        let existing = self.store.value(&key).await;

        if existing.is_none() && !(options.create || options.create_new || options.write) {
            anyhow::bail!("variable introuvable : {path}");
        }

        let bytes = existing.map(|value| encode_value(&value)).unwrap_or_default();
        let cursor = std::io::Cursor::new(bytes);
        let write = options.write || options.append || options.create || options.create_new;

        Ok(Box::pin(VarDescriptor { cursor, write, key, store: self.store.clone(), flush: None }))
    }

    async fn remove(&self, path: &str) -> anyhow::Result<()> {
        let key = path_to_key(path);
        // Supprime la valeur elle-même et toutes celles nichées dessous
        // (dossier récursif, comme `rm -r`) — un store clé-valeur plat n'a
        // pas de suppression "en bloc" par préfixe, donc une clé à la fois.
        let prefix = format!("{key}.");
        for existing_key in self.store.values().await.into_keys() {
            if existing_key == key || existing_key.starts_with(&prefix) {
                self.store.remove_value(&existing_key).await?;
            }
        }
        Ok(())
    }
}

type FlushFuture = Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;

/// Descripteur retourné par [`VarFileSystem::open`] : les octets écrits sont
/// bufferisés en mémoire (une variable tient toujours en RAM, contrairement
/// au contenu de `/files`) et flushés vers le [`VarStore`] au
/// [`AsyncWrite::poll_shutdown`] — le seul point où `tokio::io` laisse le
/// temps à une opération asynchrone de se terminer avant de considérer
/// l'écriture close.
struct VarDescriptor {
    cursor: std::io::Cursor<Vec<u8>>,
    write: bool,
    key: String,
    store: Arc<dyn VarStore>,
    flush: Option<FlushFuture>,
}

fn unsupported(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Unsupported, message.to_string())
}

impl AsyncRead for VarDescriptor {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().cursor).poll_read(cx, buf)
    }
}

impl AsyncSeek for VarDescriptor {
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.get_mut().cursor).start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.get_mut().cursor).poll_complete(cx)
    }
}

impl AsyncWrite for VarDescriptor {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        let this = self.get_mut();
        if !this.write {
            return Poll::Ready(Err(unsupported("écriture non supportée sur un descripteur ouvert en lecture")));
        }
        Pin::new(&mut this.cursor).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.write {
            return Poll::Ready(Ok(()));
        }

        loop {
            if let Some(flush) = &mut this.flush {
                return match flush.as_mut().poll(cx) {
                    Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                    Poll::Ready(Err(error)) => Poll::Ready(Err(std::io::Error::other(error))),
                    Poll::Pending => Poll::Pending,
                };
            }

            let value = decode_value(this.cursor.get_ref());
            let store = this.store.clone();
            let key = this.key.clone();
            this.flush = Some(Box::pin(async move { store.set_value(&key, value).await }));
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        sync::Mutex,
    };

    use super::*;

    #[derive(Default)]
    struct MemoryVarStore(Mutex<HashMap<String, Value>>);

    #[async_trait]
    impl VarStore for MemoryVarStore {
        async fn value(&self, key: &str) -> Option<Value> {
            self.0.lock().await.get(key).cloned()
        }

        async fn set_value(&self, key: &str, value: Value) -> anyhow::Result<()> {
            self.0.lock().await.insert(key.to_string(), value);
            Ok(())
        }

        async fn remove_value(&self, key: &str) -> anyhow::Result<()> {
            self.0.lock().await.remove(key);
            Ok(())
        }

        async fn values(&self) -> HashMap<String, Value> {
            self.0.lock().await.clone()
        }
    }

    fn filesystem() -> VarFileSystem {
        VarFileSystem::new(Arc::new(MemoryVarStore::default()))
    }

    #[test]
    fn test_path_to_key_joins_segments_with_dots() {
        assert_eq!(path_to_key("/foo/bar"), "foo.bar");
        assert_eq!(path_to_key("/"), "");
    }

    #[test]
    fn test_decode_value_parses_json_scalars_but_falls_back_to_string() {
        assert_eq!(decode_value(b"1"), Value::from(1));
        assert_eq!(decode_value(b"true"), Value::from(true));
        assert_eq!(decode_value(b"bonjour"), Value::from("bonjour"));
    }

    #[tokio::test]
    async fn test_write_then_read_round_trip_as_number() {
        let fs = filesystem();

        let mut descriptor = fs.open("/foo/bar", OpenOptions::builder().create(true).build()).await.unwrap();
        descriptor.write_all(b"1").await.unwrap();
        descriptor.shutdown().await.unwrap();

        assert_eq!(fs.store.value("foo.bar").await, Some(Value::from(1)));

        let mut descriptor = fs.open("/foo/bar", OpenOptions::builder().read(true).build()).await.unwrap();
        let mut content = String::new();
        descriptor.read_to_string(&mut content).await.unwrap();
        assert_eq!(content, "1");
    }

    #[tokio::test]
    async fn test_open_unknown_without_create_fails() {
        let fs = filesystem();
        assert!(fs.open("/missing", OpenOptions::builder().read(true).build()).await.is_err());
    }

    #[tokio::test]
    async fn test_mkdir_is_idempotent_and_visible_in_parent_ls() {
        let fs = filesystem();
        fs.mkdir("/dir").await.unwrap();
        fs.mkdir("/dir").await.unwrap();

        assert_eq!(fs.ls("/").await.unwrap(), vec!["dir".to_string()]);
    }

    #[tokio::test]
    async fn test_ls_reduces_flat_values_to_first_level_children() {
        let fs = filesystem();
        let mut a = fs.open("/foo/a", OpenOptions::builder().create(true).build()).await.unwrap();
        a.write_all(b"1").await.unwrap();
        a.shutdown().await.unwrap();
        let mut b = fs.open("/foo/b", OpenOptions::builder().create(true).build()).await.unwrap();
        b.write_all(b"2").await.unwrap();
        b.shutdown().await.unwrap();

        let mut children = fs.ls("/foo").await.unwrap();
        children.sort();
        assert_eq!(children, vec!["a".to_string(), "b".to_string()]);
        assert!(fs.ls("/").await.unwrap().contains(&"foo".to_string()));
    }
}
