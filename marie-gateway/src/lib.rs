use std::sync::Arc;

use futures::future::BoxFuture;
use marie_core::{layer::Layer, network::actor::{NetworkCommand, NetworkEvent}, session::{SessionEvent, SessionId}, workspace::{WorkspaceEvent, WorkspaceId}};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ForeignCommand {
    AccessSession(SessionId),
    AccessWorkspace(WorkspaceId)
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ForeignEvent {
    Session(SessionEvent),
    Workspace(WorkspaceEvent)
}

pub enum LocalCommand {
    
}

pub struct GatewayArgs {
    check_session: Arc<dyn Fn(SessionId) -> BoxFuture<'static, anyhow::Result<bool>>>,
    check_workspace: Arc<dyn Fn(WorkspaceId) -> BoxFuture<'static, anyhow::Result<bool>>>
}

pub struct GatewayActor;

impl GatewayActor {
    pub fn create(
        layer: impl Layer<Send = NetworkCommand, Received = NetworkEvent>,
        foreign: impl Layer<Send = GatewayEvent, Received = GatewayCommand>
    ) {
        let (tx, rx) = layer.boxed_split();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(event) = rx.event => {
                        

                    }
                }
            }
        });


    }
}