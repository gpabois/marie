use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::state::State;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Operator {
    #[serde(rename="==")]
    Equals,
    #[serde(rename="!=")]
    NotEquals,
    #[serde(rename=">")]
    GreaterThan,
    #[serde(rename=">=")]
    GreaterThanOrEqual,
    #[serde(rename="<")]
    LessThan,
    #[serde(rename="<=")]
    LessThanOrEqual,
    #[serde(rename="in")]
    Contains,
}


#[derive(Clone, Serialize, Deserialize)]
pub struct BinopCondition {
    path: String,
    op: Operator,
    value: Value
}

impl BinopCondition {
    
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Condition {
    And(Vec<Condition>),
    Or(Vec<Condition>),
    BinOp(BinopCondition)
}

impl Condition {
    pub fn check(&self, state: &State) -> bool {
        match self {
            Condition::And(conditions) => conditions.iter().all(|cond| cond.check(state)),
            Condition::Or(conditions) => conditions.iter().any(|cond| cond.check(state)),
            Condition::BinOp(binop_condition) => todo!(),
        }
    }
}
