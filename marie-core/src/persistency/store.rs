use std::path::Path;
use std::sync::Arc;

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

const KV_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

/// Moteur de stockage clé-valeur embarqué (`redb`, fichier unique, sans
/// process serveur à administrer) — backend local par défaut de chaque trait
/// CRUD spécifique du cluster (`persistency::SessionStore`,
/// `persistency::WorkspaceStore`, `model::catalog::store::ModelStore`,
/// `tools::catalog::store::ToolStore`, `expert::catalog::store::ExpertStore`,
/// `mode::state_graph::catalog::store::StateGraphStore`), alternative à
/// [`super::postgres::PostgresStore`] pour les déploiements sans
/// infrastructure partagée.
///
/// Volontairement pas de trait CRUD générique par-dessus ce type : chaque
/// objet du domaine a son propre trait, avec ses propres méthodes, implémenté
/// directement pour [`RedbStore`] là où il est défini (voir
/// `persistency::session`/`persistency::workspace`,
/// `model`/`tools`/`expert`/`mode::state_graph::catalog::store`). Une seule table `redb` héberge
/// néanmoins tous ces types, distingués par un préfixe de clé
/// (`namespace/id`) plutôt qu'une table par type — `redb` n'a pas de notion
/// de table nommée dynamiquement aussi bon marché qu'un préfixe de clé — d'où
/// les méthodes `*_raw` ci-dessous, partagées entre ces implémentations pour
/// ne pas dupliquer la plomberie `redb` (transaction, `spawn_blocking`) à
/// chaque type.
///
/// Les opérations `redb` sont synchrones (E/S fichier bloquantes) : chaque
/// appel est délégué à [`tokio::task::spawn_blocking`] pour ne pas bloquer
/// le runtime asynchrone.
pub struct RedbStore {
    db: Arc<redb::Database>,
}

impl RedbStore {
    pub fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let db = redb::Database::create(path)?;

        // Crée la table dès l'ouverture : évite d'avoir à distinguer "table
        // absente" de "clé absente" dans `get_raw`.
        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(KV_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    /// Valeur brute associée à `namespace/id`, si connue.
    pub(crate) async fn get_raw(&self, namespace: &str, id: &str) -> anyhow::Result<Option<Vec<u8>>> {
        let db = self.db.clone();
        let key = raw_key(namespace, id);

        tokio::task::spawn_blocking(move || -> anyhow::Result<Option<Vec<u8>>> {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(KV_TABLE)?;
            Ok(table.get(key.as_slice())?.map(|value| value.value().to_vec()))
        })
        .await?
    }

    pub(crate) async fn put_raw(&self, namespace: &str, id: &str, value: Vec<u8>) -> anyhow::Result<()> {
        let db = self.db.clone();
        let key = raw_key(namespace, id);

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(KV_TABLE)?;
                table.insert(key.as_slice(), value.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    pub(crate) async fn delete_raw(&self, namespace: &str, id: &str) -> anyhow::Result<()> {
        let db = self.db.clone();
        let key = raw_key(namespace, id);

        tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
            let write_txn = db.begin_write()?;
            {
                let mut table = write_txn.open_table(KV_TABLE)?;
                table.remove(key.as_slice())?;
            }
            write_txn.commit()?;
            Ok(())
        })
        .await?
    }

    /// Toutes les valeurs brutes dont la clé commence par `namespace/`.
    pub(crate) async fn list_raw(&self, namespace: &str) -> anyhow::Result<Vec<Vec<u8>>> {
        let db = self.db.clone();
        let prefix = format!("{namespace}/").into_bytes();

        tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<Vec<u8>>> {
            let read_txn = db.begin_read()?;
            let table = read_txn.open_table(KV_TABLE)?;

            let mut matches = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                if key.value().starts_with(prefix.as_slice()) {
                    matches.push(value.value().to_vec());
                }
            }
            Ok(matches)
        })
        .await?
    }
}

fn raw_key(namespace: &str, id: &str) -> Vec<u8> {
    format!("{namespace}/{id}").into_bytes()
}
