package com.cs542.app;


import java.util.Objects;

public class Operation implements java.io.Serializable {

    public String key;
    public String value; // only applies to APPEND
    public OpType type;
    public TransactionId transactionId;
    //    public Lock lock; // every operation of every transaction will need to get a lock
    public int index; // order in current transaction

    public Operation(String key, String value, OpType type, TransactionId transactionId, int index) {
        this.key = key;
        this.value = value;
        this.type = type;
        this.transactionId = transactionId;
//        this.lock = new Lock(getLockType(), key, transactionId);
        this.index = index;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public OpType getType() {
        return type;
    }

    public void setType(OpType type) {
        this.type = type;
    }

    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    @Override
    public String toString() {
        return "[(" + key + "->" + value + "), id: " + transactionId.toString() + "(opIdx:" + index + "), type: " + type + "]";
    }

    public LockType getLockType() {
        switch (this.type) {
            case GET:
                return LockType.READ;
            case PUT:
            case APPEND:
                return LockType.WRITE;
            default:
                return null;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, transactionId, index);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Operation) {
            Operation opObj = (Operation) obj;
            return opObj.getTransactionId().equals(transactionId) && opObj.getKey().equals(key) && opObj.getValue().equals(value)
                    && opObj.getIndex() == index;
        }
        return false;
    }
}
