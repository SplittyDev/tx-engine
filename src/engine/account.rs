use std::collections::HashMap;

use serde::{ser::SerializeStruct, Serialize, Serializer};

#[derive(Debug, Clone)]
pub struct TransactionDetails {
    pub amount: f32,
    pub disputed: bool,
}

impl TransactionDetails {
    pub fn new(amount: f32) -> Self {
        TransactionDetails {
            amount,
            disputed: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Account {
    pub client_id: u16,
    pub held_balance: f32,
    pub available_balance: f32,
    pub locked: bool,
    pub transactions: HashMap<u32, TransactionDetails>,
}

impl Account {
    pub fn new(client_id: u16) -> Self {
        Account {
            client_id,
            held_balance: 0.0,
            available_balance: 0.0,
            locked: false,
            transactions: HashMap::new(),
        }
    }

    pub fn total_balance(&self) -> f32 {
        self.available_balance + self.held_balance
    }
}

impl Serialize for Account {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Account", 5)?;
        state.serialize_field("client", &self.client_id)?;
        state.serialize_field("available", &self.available_balance)?;
        state.serialize_field("held", &self.held_balance)?;
        state.serialize_field("total", &self.total_balance())?;
        state.serialize_field("locked", &self.locked)?;
        state.end()
    }
}
