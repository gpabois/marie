use marie_macros::core_rpc;

use crate::rpc::Void;
use super::{capabilities::Capabilities, Annuary};

core_rpc! {
    #[rpc(name = "/marie/annuary/capabilities/get")]
    pub async fn get_capabilities(self: Self<Annuary>, _: Void) -> Capabilities {
        *self.0.capabilities
    }
}