use serde::{Deserialize, Serialize};


#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FramePolicy {
    pub child_failure_policy: ChildFailurePolicy,
    pub parent_policy: ParentPolicy
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildFailurePolicy {
    FailIfAtLeastHasFailed(usize),
    #[default]
    DontFail
}

#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ParentPolicy {
    #[default]
    // Reduce only on the last terminated child frame
    Sequential,
    // Reduce when all the child frames have terminated.
    FanIn,
}
