use marie_macros::core_rpc;

use crate::{hitl::protocol::HitlResponse, session::SessionId};
use crate::session::controller::SessionController;

use super::protocol::NewSessionArgs;

core_rpc! {
    #[rpc(name="/marie/sessions/create")]
    pub async fn create_session(self: Self<SessionController>, args: NewSessionArgs) -> Result<SessionId, String> {
        self.0.create_session(args).await.map_err(|err| err.to_string())
    }
}

core_rpc! {
    #[rpc(name="/marie/sessions/reply-hitl")]
    pub async fn reply_hitl(self: Self<SessionController>, response: HitlResponse) -> Result<(), String> {
        self.0.reply_hitl(response).await.map_err(|err| err.to_string())
    }
}