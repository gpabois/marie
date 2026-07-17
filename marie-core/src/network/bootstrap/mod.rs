pub mod client;

pub use client::{BootstrapClient, BootstrapClientActor};

use crate::{layer::{IntoService, Layer}, network::{actor::{Network, NetworkCommand, NetworkEvent}, bootstrap::client::BootstrapArgs}};

impl<L> IntoService<BootstrapClient, BootstrapArgs> for L where L: Layer<Send=NetworkCommand, Received = NetworkEvent> {
    fn into_service(self, args: BootstrapArgs) -> BootstrapClient {
        BootstrapClientActor::new(self, args)
    }
}

pub fn build_client(net: &Network, args: BootstrapArgs) -> BootstrapClient {
    net.transport().into_service(args)
}