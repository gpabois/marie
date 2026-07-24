use anyhow::{anyhow, bail};
use bytemuck::{Pod, Zeroable};
use rand_xoshiro::{
    Xoshiro256PlusPlus,
    rand_core::{Rng as _, SeedableRng as _},
};
use schemars::JsonSchema;
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct IdGenerator {
    // Utilisation de RefCell pour une mutation intérieure sûre en single-thread (WASM/Client)
    rng: Arc<Mutex<Xoshiro256PlusPlus>>,
    session_prefix: u64,
}

#[derive(Hash, Debug, Clone, Copy, PartialEq, Eq, Pod, Zeroable, JsonSchema)]
#[repr(C)]
pub struct ID(u64, u64);

impl serde::Serialize for ID {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for ID {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = String::deserialize(deserializer)?;
        repr.parse().map_err(serde::de::Error::custom)
    }
}

impl ID {
    pub fn as_bytes(&self) -> &[u8] {
        self.as_ref()
    }
}

impl TryFrom<&[u8]> for ID {
    type Error = anyhow::Error;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        bytemuck::try_from_bytes(bytes).copied().map_err(|_| {
            anyhow!("La slice d'octets n'a pas la bonne taille ou un mauvais alignement")
        })
    }
}

impl AsRef<[u8]> for ID {
    fn as_ref(&self) -> &[u8] {
        // bytemuck::bytes_of convertit de manière sûre la référence en &[u8]
        bytemuck::bytes_of(self)
    }
}

impl std::fmt::Display for ID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&format!("{:016x}{:016x}", self.0, self.1))
    }
}

impl FromStr for ID {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() != 32 {
            bail!("ID parsing error : expecting a 32 long string")
        }
        let id1 = u64::from_str_radix(&s[0..16], 16)?;
        let id2 = u64::from_str_radix(&s[16..32], 16)?;
        Ok(Self(id1, id2))
    }
}

impl Default for IdGenerator {
    /// Initialise le générateur avec une seed unique par session (via getrandom)
    fn default() -> Self {
        // 1. On génère une seed aléatoire sécurisée pour cette session
        let mut seed = [0u8; 32];

        if getrandom::getrandom(&mut seed).is_err() {
            // Fallback dégradé au cas où, mais js/getrandom gère ça sur le web
            seed = [42; 32];
        }

        // 2. On instancie le RNG avec cette seed
        let mut master_rng = Xoshiro256PlusPlus::from_seed(seed);

        // 3. On extrait un préfixe unique pour cette session
        let session_prefix = master_rng.next_u64();

        Self {
            rng: Arc::new(Mutex::new(master_rng)),
            session_prefix,
        }
    }
}

impl IdGenerator {
    /// Génère un identifiant unique sous forme de chaîne (ex: "id-session-séquence")
    pub fn next_id_str(&self) -> String {
        let mut rng = self.rng.lock().unwrap();
        let local_id = rng.next_u64();
        // Le format combine le préfixe de session et un nombre aléatoire local
        format!("{:x}{:x}", self.session_prefix, local_id)
    }

    pub fn next_id(&self) -> ID {
        let mut rng = self.rng.lock().unwrap();
        let local_id = rng.next_u64();
        ID(self.session_prefix, local_id)
    }
}

pub fn generate_id() -> ID {
    IdGenerator::default().next_id()
}
