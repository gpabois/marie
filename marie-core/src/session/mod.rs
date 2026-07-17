use bytemuck::{Pod, Zeroable};
use serde::{Deserialize, Serialize};

use crate::id::ID;

#[derive(Debug, Hash, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Pod, Zeroable)]
#[repr(C)]
pub struct SessionId(ID);