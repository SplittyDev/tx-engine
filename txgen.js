#!/usr/bin/env node

console.log('type,client,tx,amount')

const lut = {
  deposit: (clientId, txId, amount) => `deposit,${clientId},${txId},${amount}`,
  withdraw: (clientId, txId, amount) => `withdrawal,${clientId},${txId},${amount}`
}

let clientId = 0
let maxClientId = 0

const disputedTxIds = []
const output = []

for (let txId = 0; txId < 1000000; txId++) {
  // Generate random deposit or withdrawal
  const lutIndex = Math.floor(Math.random() * Object.keys(lut).length)
  const lutFn = Object.values(lut)[lutIndex]
  if (Math.random() < 0.75) {
    maxClientId = Math.max(maxClientId, ++clientId)
  } else {
    clientId = Math.floor(Math.random() * maxClientId)
  }
  output.push(lutFn(clientId, txId, (Math.random() + 1) * 100))

  // Once in a blue moon, generate a dispute
  if (Math.random() < 0.1) {
    output.push(`dispute,${clientId},${txId}`)
    disputedTxIds.push(txId)
  }

  // Once in a blue moon, resolve a dispute
  if (disputedTxIds.length > 0 && Math.random() < 0.1) {
    const disputeIndex = Math.floor(Math.random() * disputedTxIds.length)
    const disputeTxId = disputedTxIds[disputeIndex]
    disputedTxIds.splice(disputeIndex, 1)
    output.push(`resolve,${clientId},${disputeTxId}`)
  }

  // Once in a blue moon, issue a chargeback
  if (disputedTxIds.length > 0 && Math.random() < 0.1) {
    const disputeIndex = Math.floor(Math.random() * disputedTxIds.length)
    const disputeTxId = disputedTxIds[disputeIndex]
    disputedTxIds.splice(disputeIndex, 1)
    output.push(`chargeback,${clientId},${disputeTxId}`)
  }
}

console.log(output.join('\n'))
