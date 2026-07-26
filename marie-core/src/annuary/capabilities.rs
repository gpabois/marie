use std::{collections::HashSet, sync::Arc};

use marie_macros::core_rpc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[repr(u8)]
pub enum Capability {
    Gateway = 0b01,
    Orchestrator = 0b10,
    Worker = 0b100,
    State = 0b1000,
    /// Le nœud participe au groupe Raft de [`crate::lease::authority::LeaseAuthority`]
    /// (bail/epoch des sessions) — voir `lease::server::build_lease_authority`.
    /// Rôle purement déclaratif : n'importe quel nœud peut porter ce bit,
    /// aucun `NodeKind` dédié n'est nécessaire.
    Lease = 0b10000
}

impl From<Capability> for u8 {
    fn from(value: Capability) -> Self {
        value as u8
    }
}


#[derive(Default, Clone, Copy, Serialize, Deserialize)]
pub struct Capabilities(u8);

impl Capabilities {
    pub fn includes(&self, capabilites: Capabilities) -> bool {
        self.0 & capabilites.0 == capabilites.0
    }
    
    pub fn gateway(&self) -> bool {
        self.0 & (&Capability::Gateway.into()) > 0
    }

    pub fn orchestrator(&self) -> bool {
        self.0 & (&Capability::Orchestrator.into()) > 0
    }

    pub fn worker(&self) -> bool {
        self.0 & (&Capability::Worker.into()) > 0
    }

    pub fn state(&self) -> bool {
        self.0 & (&Capability::State.into()) > 0
    }

    pub fn lease(&self) -> bool {
        self.0 & (&Capability::Lease.into()) > 0
    }

    pub fn set_gateway(&mut self) {
        self.0 |= &Capability::Gateway.into();
    }

    pub fn set_orchestrator(&mut self) {
        self.0 |= &Capability::Orchestrator.into();
    }

    pub fn set_worker(&mut self) {
        self.0 |= &Capability::Worker.into();
    }

    pub fn set_state(&mut self) {
        self.0 |= &Capability::State.into();
    }

    pub fn set_lease(&mut self) {
        self.0 |= &Capability::Lease.into();
    }
}

