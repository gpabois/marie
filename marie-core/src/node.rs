use crate::{di::Container, network::Network, secret::{KeyEpoch, SecretKey}};

pub struct MarieNodeArgs {
    epochs: Vec<(KeyEpoch, SecretKey)>,
    current_epoch: KeyEpoch
}
