use anyhow::{anyhow, Context, Result};
use std::{
    error::Error,
    marker::{Send, Sync},
    sync::{Arc, Mutex, RwLock},
};

use super::{Account, TransactionDetails, TransactionRecord, TransactionType};

/// A small bridge between the `TransactionEngine` and the `Account`.
///
/// This allows the engine to find accounts by `client_id` without having to
/// acquire locks for all accounts.
struct AccountAccessor {
    client_id: u16,
    account: Arc<Mutex<Account>>,
}

/// The heart of the transaction processing logic.
pub struct TransactionEngine {
    accounts: RwLock<Vec<AccountAccessor>>,
}

impl TransactionEngine {
    /// Construct a new `TransactionEngine`.
    pub fn new() -> Self {
        TransactionEngine {
            accounts: RwLock::new(Vec::new()),
        }
    }

    /// Process all transaction records from the given iterator.
    pub async fn process_records<I, E>(&self, records: I) -> Result<()>
    where
        I: Iterator<Item = std::result::Result<TransactionRecord, E>> + Sync + Send,
        E: Error + Sync + Send + 'static,
    {
        for record in records {
            Self::process_transaction(&self.accounts, record?).await?
        }

        Ok(())
    }

    /// Return a collection of all accounts.
    ///
    /// This method should only be called after all `process_records` calls have ended.
    /// It's very expensive, since it acquires locks for all accounts.
    pub fn accounts(&self) -> Result<Vec<Account>> {
        let accounts = self
            .accounts
            .read()
            .map_err(|_| anyhow!("Unable to read accounts."))?;
        Ok(accounts
            .iter()
            .map(|accessor| accessor.account.lock().unwrap().clone())
            .collect())
    }

    /// Process a single transaction record.
    async fn process_transaction(
        accounts: &RwLock<Vec<AccountAccessor>>,
        tx: TransactionRecord,
    ) -> Result<()> {
        // Validate transaction
        if !tx.is_valid() {
            return Err(anyhow!("Invalid transaction."));
        }

        // Check whether the client already has an account
        let client_exists = {
            accounts
                .read()
                .map_err(|_| anyhow!("Unable to acquire read-lock on accounts."))?
                .iter()
                .any(|a| a.client_id == tx.client_id)
        };

        // Create account if it doesn't exist
        if !client_exists {
            let account = Account::new(tx.client_id);
            let account_accessor = AccountAccessor {
                client_id: tx.client_id,
                account: Arc::new(Mutex::new(account)),
            };
            let mut accounts = accounts
                .write()
                .map_err(|_| anyhow!("Unable to acquire write-lock on accounts."))?;
            accounts.push(account_accessor);
        }

        // Acquire read-lock on accounts
        let read_only_accounts = accounts
            .read()
            .map_err(|_| anyhow!("Unable to acquire read-lock on accounts."))?;

        // Find the account for the current transaction
        let account_accessor = read_only_accounts
            .iter()
            .find(|a| a.client_id == tx.client_id)
            .context("Unable to locale account by client id.")?;

        // Acquire a lock on the account
        let mut acc = account_accessor
            .account
            .lock()
            .map_err(|_| anyhow!("Unable to acquire mutable account reference."))?;

        // Check if account is locked
        if acc.locked {
            // Don't process transaction and return
            return Ok(());
        }

        // Record transaction if it has an amount (deposit, withdrawal)
        if let Some(amount) = tx.amount {
            acc.transactions
                .insert(tx.transaction_id, TransactionDetails::new(amount));
        }

        match tx.r#type {
            // Handle deposit
            TransactionType::Deposit => {
                let amount = tx
                    .amount
                    .context("Unable to get amount from transaction.")?;
                acc.available_balance += amount;
            }

            // Handle withdrawal
            TransactionType::Withdraw => {
                let amount = tx
                    .amount
                    .context("Unable to get amount from transaction.")?;

                // Check for sufficient funds
                if (acc.available_balance - amount).is_sign_negative() {
                    // Insufficient funds. Stop withdrawal but don't error out.
                    return Ok(());
                }

                acc.available_balance -= amount;
            }

            // Handle dispute
            TransactionType::Dispute => {
                // Find original transaction
                if let Some(original_tx) = acc.transactions.get(&tx.transaction_id).cloned() {
                    // This case is not listed in the specification, I'm assuming this is a no-op.
                    if original_tx.disputed {
                        return Ok(());
                    }

                    // Mark transaction as disputed
                    acc.transactions
                        .get_mut(&tx.transaction_id)
                        .context("Unable to get transaction details.")?
                        .disputed = true;

                    // Freeze transaction amount
                    acc.available_balance -= original_tx.amount;
                    acc.held_balance += original_tx.amount;
                } else {

                    // Transaction doesn't exist. As per specification,
                    // this is an error on the partner side and should be ignored.
                }
            }

            // Handle dispute resolution
            TransactionType::Resolve => {
                // Find original transaction
                if let Some(original_tx) = acc.transactions.get(&tx.transaction_id).cloned() {
                    // Ignore resolutions for undisputed transactions
                    if !original_tx.disputed {
                        return Ok(());
                    }

                    // Mark transaction as resolved
                    acc.transactions
                        .get_mut(&tx.transaction_id)
                        .context("Unable to get transaction details.")?
                        .disputed = false;

                    // Release transaction amount
                    acc.available_balance += original_tx.amount;
                    acc.held_balance -= original_tx.amount;
                } else {

                    // Transaction doesn't exist. As per specification,
                    // this is an error on the partner side and should be ignored.
                }
            }

            // Handle chargeback
            TransactionType::Chargeback => {
                // Find original transaction
                if let Some(original_tx) = acc.transactions.get(&tx.transaction_id).cloned() {
                    // Ignore chargeback for undisputed transactions
                    if !original_tx.disputed {
                        return Ok(());
                    }

                    // Mark transaction as resolved
                    acc.transactions
                        .get_mut(&tx.transaction_id)
                        .context("Unable to get transaction details.")?
                        .disputed = false;

                    // Remove backcharged balance
                    acc.held_balance -= original_tx.amount;

                    // Lock account
                    acc.locked = true;
                } else {

                    // Transaction doesn't exist. As per specification,
                    // this is an error on the partner side and should be ignored.
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::TransactionEngine;
    use tokio::test;

    // Helper macro to read transaction records from a string, process them and compare them to the expected output.
    macro_rules! assert_csv_snapshot {
        ($csv:expr => $expected:expr) => {{
            // We can pretty much unwrap everything here, if anything goes wrong the test will fail.
            // This is the correct behavior since any failure means something is seriously wrong.
            let engine = TransactionEngine::new();
            let input: String = $csv
                .split_whitespace()
                .map(|s| format!("{}\n", s))
                .collect();
            let expected_output: String = $expected
                .split_whitespace()
                .map(|s| format!("{}\n", s))
                .collect();
            let reader = csv::ReaderBuilder::new()
                .delimiter(b',')
                .flexible(true)
                .trim(csv::Trim::All)
                .from_reader(input.as_bytes());
            engine
                .process_records(reader.into_deserialize())
                .await
                .unwrap();
            let mut accounts = engine.accounts().unwrap();
            accounts.sort_by(|a, b| a.client_id.partial_cmp(&b.client_id).unwrap());
            let mut output_writer = csv::WriterBuilder::new()
                .delimiter(b',')
                .has_headers(true)
                .flexible(false)
                .from_writer(vec![]);
            for account in accounts {
                output_writer.serialize(account).unwrap();
            }
            let output = String::from_utf8(output_writer.into_inner().unwrap()).unwrap();
            if output != expected_output {
                println!("Actual: {}", output);
            }
            assert!(output == expected_output);
        }};
    }

    #[test]
    async fn test_tx_deposits_one_client() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,1.0
                deposit,1,2,2.0
                deposit,1,3,2.0
                deposit,1,4,20.5
            "
            =>
            "
                client,available,held,total,locked
                1,25.5,0.0,25.5,false
            "
        )
    }

    #[test]
    async fn test_tx_deposits_withdrawals_positive_balance() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                withdrawal,1,3,15.0
            "
            =>
            "
                client,available,held,total,locked
                1,20.0,0.0,20.0,false
            "
        )
    }

    #[test]
    async fn test_tx_deposits_withdrawals_zero_balance() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                withdrawal,1,3,35.0
            "
            =>
            "
                client,available,held,total,locked
                1,0.0,0.0,0.0,false
            "
        )
    }

    #[test]
    async fn test_tx_deposits_withdrawals_negative_balance() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                withdrawal,1,3,40.0
            "
            =>
            "
                client,available,held,total,locked
                1,35.0,0.0,35.0,false
            "
        )
    }

    #[test]
    async fn test_tx_dispute() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                dispute,1,2,
            "
            =>
            "
                client,available,held,total,locked
                1,10.0,25.0,35.0,false
            "
        )
    }

    #[test]
    async fn test_tx_with_invalid_dispute() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                dispute,1,3,
            "
            =>
            "
                client,available,held,total,locked
                1,35.0,0.0,35.0,false
            "
        )
    }

    #[test]
    async fn test_tx_dispute_with_resolution() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                dispute,1,2,
                deposit,1,3,10.0
                resolve,1,2,
            "
            =>
            "
                client,available,held,total,locked
                1,45.0,0.0,45.0,false
            "
        )
    }

    #[test]
    async fn test_tx_dispute_with_invalid_resolution() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                dispute,1,2,
                deposit,1,3,10.0
                resolve,1,4,
            "
            =>
            "
                client,available,held,total,locked
                1,20.0,25.0,45.0,false
            "
        )
    }

    #[test]
    async fn test_tx_undisputed_resolution() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                resolve,1,2,
            "
            =>
            "
                client,available,held,total,locked
                1,35.0,0.0,35.0,false
            "
        )
    }

    #[test]
    async fn test_tx_dispute_with_chargeback() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                dispute,1,2,
                chargeback,1,2,
            "
            =>
            "
                client,available,held,total,locked
                1,10.0,0.0,10.0,true
            "
        )
    }

    #[test]
    async fn test_tx_with_undisputed_chargeback() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                chargeback,1,2,
            "
            =>
            "
                client,available,held,total,locked
                1,35.0,0.0,35.0,false
            "
        )
    }

    #[test]
    async fn test_tx_deposit_with_locked_account() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,1,2,25.0
                dispute,1,2,
                chargeback,1,2,
                deposit,1,3,50.0
            "
            =>
            "
                client,available,held,total,locked
                1,10.0,0.0,10.0,true
            "
        )
    }

    #[test(flavor = "multi_thread")]
    async fn test_multiple_deposits_withdrawals() {
        assert_csv_snapshot!(
            "
                type,client,tx,amount
                deposit,1,1,10.0
                deposit,2,2,10.0
                deposit,1,3,10.0
                deposit,5,4,10.0
                deposit,4,5,10.0
                withdrawal,1,6,20.0
                deposit,6,7,10.0
                deposit,3,8,20.0
                dispute,2,2
                deposit,3,9,5.0
                deposit,2,10,10.0
                resolve,2,2
                dispute,3,8
                deposit,7,11,15.0
                dispute,7,11
                chargeback,7,11
                deposit,7,12,20.0
            "
            =>
            "
                client,available,held,total,locked
                1,0.0,0.0,0.0,false
                2,20.0,0.0,20.0,false
                3,5.0,20.0,25.0,false
                4,10.0,0.0,10.0,false
                5,10.0,0.0,10.0,false
                6,10.0,0.0,10.0,false
                7,0.0,0.0,0.0,true
            "
        )
    }
}
