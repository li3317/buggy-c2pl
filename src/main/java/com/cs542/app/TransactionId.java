package com.cs542.app;

import java.util.Objects;

public class TransactionId implements java.io.Serializable {
    private int seqNum;
    private int siteId;
    public int index; // assigned by central site, should be non-negative int

    public TransactionId(int seqNum, int siteId) {
        this.seqNum = seqNum;
        this.siteId = siteId;
        this.index = -10;
    }

    public boolean isIndexSet() {
        return this.index >= 0;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public void setSeqNum(int seqNum) {
        this.seqNum = seqNum;
    }

    public int getSiteId() {
        return siteId;
    }

    public void setSiteId(int siteId) {
        this.siteId = siteId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seqNum, siteId);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TransactionId) {
            return ((TransactionId) obj).seqNum == this.seqNum
                    && ((TransactionId) obj).siteId == this.siteId;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Tr").append(seqNum).append("(").append(siteId).append(")");
        if (isIndexSet())
            sb.append("i(").append(this.index).append(")");
        sb.append(" [").append(this.hashCode()).append("]");
        return sb.toString();
    }
}
