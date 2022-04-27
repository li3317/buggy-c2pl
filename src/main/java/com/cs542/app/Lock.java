package com.cs542.app;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

/* each operation in each transaction needs to acquire a lock, lock type can be upgraded */
public class Lock implements java.io.Serializable {
    private static Logger LOG = null;

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format",
                "[%1$tF %1$tT] [%4$-7s] [Lock] %5$s %n");
        LOG = Logger.getLogger(Lock.class.getName());
    }

    public String key;
    public LockType type;
    public TransactionId transaction;

    public Lock(LockType type, String key, TransactionId id) { // create a lock for an item, usage: add to transactions list
        this.type = type;
        this.key = key;
        this.transaction = id;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public LockType getType() {
        return type;
    }

    public void setType(LockType type) {
        this.type = type;
    }

    public TransactionId getTransaction() {
        return transaction;
    }

    public void setTransaction(TransactionId transaction) {
        this.transaction = transaction;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        builder.append(key + " (");

        switch(type) {
            case READ:
                builder.append("read");
                break;
            case WRITE:
                builder.append("write");
                break;
            default:
        }
        builder.append(") -> ");
        builder.append(transaction.toString()).append("]");
        return builder.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, type, transaction);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Lock) {
            Lock temp = (Lock) obj;
            return this.transaction.equals(temp.transaction)
                    && this.key.equals(temp.key) && this.type.equals(temp.type);
        }
        return false;
    }

    /* if a transaction is holding a read lock and wants a write lock, upgrade it */
    public void upgradeType(LockType newType) {
        if (this.type == LockType.READ && newType == LockType.WRITE) {
            this.type = LockType.WRITE;
        }
    }
}
