use crate::secret::{KeyEpoch, SecretManager};

use super::store::{ModelStore, ModelStoreClient, StoredModel};

/// Résultat d'une passe de [`reencrypt_model_store`] — étape 4 du runbook de
/// rotation (voir doc de [`crate::secret::SecretManager`]).
#[derive(Default, Debug, Clone, Copy)]
pub struct ReencryptReport {
    pub migrated: usize,
    pub already_current: usize,
}

/// Re-chiffre, séquentiellement, chaque entrée du catalogue de modèles
/// encore taguée d'une epoch différente de `secret.current_epoch()` — étape
/// 4 du runbook de rotation (voir doc de [`SecretManager`]). Idempotent (les
/// entrées déjà à l'epoch courante sont ignorées via [`StoredModel::key_epoch`],
/// sans déchiffrement) et interruptible/reprenable sans risque : chaque
/// `replace` est une mise à jour atomique indépendante, relancer la passe
/// après une interruption refait juste ce même contrôle par entrée via un
/// nouveau `list()` — pas besoin de table de progression séparée à l'échelle
/// actuelle du catalogue.
///
/// Déclenchement volontairement manuel (pas de tâche planifiée
/// automatique) : c'est une action d'incident-response, à l'appelant de
/// décider quand la lancer.
pub async fn reencrypt_model_store(store: ModelStoreClient, secret: &SecretManager) -> anyhow::Result<ReencryptReport> {
    let target = secret.current_epoch();
    let mut report = ReencryptReport::default();

    for stored in store.clone().list().await? {
        if stored.key_epoch() == target {
            report.already_current += 1;
            continue;
        }

        let model = stored.decrypt(secret)?;
        let reencrypted = StoredModel::encrypt(&model, secret)?;
        store.clone().replace(reencrypted).await?;
        report.migrated += 1;
    }

    Ok(report)
}

/// Garde-fou avant [`SecretManager::retire_epoch`] (étape 5 du runbook de
/// rotation) : erreur (avec le compte d'entrées concernées) si le catalogue
/// de modèles contient encore une entrée chiffrée sous `epoch`. Ne consulte
/// que l'epoch tagué sur chaque entrée (voir [`StoredModel::key_epoch`]),
/// sans tenter de déchiffrer quoi que ce soit.
pub async fn assert_no_rows_at_epoch(store: ModelStoreClient, epoch: KeyEpoch) -> anyhow::Result<()> {
    let remaining = store
        .list()
        .await?
        .into_iter()
        .filter(|stored| stored.key_epoch() == epoch)
        .count();

    anyhow::ensure!(
        remaining == 0,
        "{remaining} entrée(s) du catalogue de modèles encore chiffrée(s) sous l'epoch {epoch} : lancer reencrypt_model_store avant de la retirer"
    );

    Ok(())
}
