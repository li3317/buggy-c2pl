package com.cs542.app;

import java.util.ArrayList;
import java.util.List;

public class Transaction {
    // each transaction request when received by each dataSite, will generate unique transaction id
    // of form sequenceNum + siteId

    public TransactionId transactionId;
    public List<Operation> operations;

    public Transaction(TransactionId transactionId) {
        this.transactionId = transactionId;
        this.operations = new ArrayList<>();
    }

    public void addOperation(Operation operation) {
        operations.add(operation);
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public List<Operation> getOperations() {
        return operations;
    }

    @Override
    public String toString() {
        return "Transaction{id=" + transactionId.toString() + ", ops=" + operations.toString() + '}';
    }
}
