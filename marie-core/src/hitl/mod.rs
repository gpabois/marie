use crate::{id::ID, session::SessionId};

#[derive(Clone, Copy)]
pub struct HitlId(SessionId, ID);