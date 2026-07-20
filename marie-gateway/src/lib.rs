use marie_core::{layer::Layer, network::actor::{NetworkCommand, NetworkEvent}, session::{SessionEvent, SessionId}, workspace::{WorkspaceEvent, WorkspaceId}};

pub enum GatewayCommand {
    AccessSession(SessionId),
    AccessWorkspace(WorkspaceId)
}
pub enum GatewayEvent {
    Session(SessionEvent),
    Workspace(WorkspaceEvent)
}

pub struct GatewayArgs {
    
}

pub struct GatewayActor;

impl GatewayActor {
    pub fn create(
        layer: impl Layer<Send = NetworkCommand, Received = NetworkEvent>,
        foreign: impl Layer<Send = GatewayEvent, Received = GatewayCommand>
    ) {
        
    }
}