use marie_macros::core_rpc;

use crate::session::SessionId;

#[cfg(feature="rpc-executor")]
use crate::session::controller::SessionController;

use super::protocol::CreateSessionArgs;

core_rpc! {
    #[rpc(name="/marie/sessions/create")]
    pub async fn create_session(self: Self<SessionController>, args: CreateSessionArgs) -> Result<SessionId, String> {
        self.0.create_session(args).await.map_err(|err| err.to_string())
    }
}