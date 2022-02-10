use serde::Deserialize;

use super::TransactionType;

#[derive(Debug, Deserialize)]
pub struct TransactionRecord {
    pub r#type: TransactionType,
    #[serde(rename = "client")]
    pub client_id: u16,
    #[serde(rename = "tx")]
    pub transaction_id: u32,
    pub amount: Option<f32>,
}

impl TransactionRecord {
    /// Validate the transaction.
    ///
    /// Rules for transaction validity:
    /// 1. `type` IN (`deposit`, `withdrawal`) AND `amount` IS present => valid
    /// 2. `type` IN (`dispute`, `resolution`, `chargeback`) AND `amount` IS NOT present => valid
    ///
    /// All other cases are invalid.
    pub fn is_valid(&self) -> bool {
        let has_amount = self.amount.is_some();

        // Define valid tx types with/without amount present
        let valid_cases_with_amount = [TransactionType::Deposit, TransactionType::Withdraw];
        let valid_cases_without_amount = [
            TransactionType::Dispute,
            TransactionType::Resolve,
            TransactionType::Chargeback,
        ];

        // Test tx type against predefined checklist
        let valid_tx_with_amount = has_amount && valid_cases_with_amount.contains(&self.r#type);
        let valid_tx_without_amount =
            !has_amount && valid_cases_without_amount.contains(&self.r#type);

        // Determine whether the transaction is valid
        valid_tx_with_amount || valid_tx_without_amount
    }
}

#[cfg(test)]
mod tests {
    use super::{TransactionRecord, TransactionType};

    #[test]
    fn test_is_valid() {
        let valid_transactions = [
            TransactionRecord {
                r#type: TransactionType::Deposit,
                client_id: 1,
                transaction_id: 1,
                amount: Some(100.0),
            },
            TransactionRecord {
                r#type: TransactionType::Withdraw,
                client_id: 1,
                transaction_id: 1,
                amount: Some(100.0),
            },
            TransactionRecord {
                r#type: TransactionType::Dispute,
                client_id: 1,
                transaction_id: 1,
                amount: None,
            },
            TransactionRecord {
                r#type: TransactionType::Chargeback,
                client_id: 1,
                transaction_id: 1,
                amount: None,
            },
        ];

        for tx in valid_transactions {
            assert!(tx.is_valid());
        }

        let invalid_transactions = [
            TransactionRecord {
                r#type: TransactionType::Deposit,
                client_id: 1,
                transaction_id: 1,
                amount: None,
            },
            TransactionRecord {
                r#type: TransactionType::Withdraw,
                client_id: 1,
                transaction_id: 1,
                amount: None,
            },
            TransactionRecord {
                r#type: TransactionType::Dispute,
                client_id: 1,
                transaction_id: 1,
                amount: Some(1.23),
            },
            TransactionRecord {
                r#type: TransactionType::Chargeback,
                client_id: 1,
                transaction_id: 1,
                amount: Some(1.23),
            },
        ];

        for tx in invalid_transactions {
            assert!(!tx.is_valid());
        }
    }
}
