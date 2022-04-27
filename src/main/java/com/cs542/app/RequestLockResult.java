package com.cs542.app;

public class RequestLockResult implements java.io.Serializable {
    public int index;
    public boolean granted;
    public RequestLockResult() {
        index = -10;
    }

    public boolean isIndexSet() {
        return index != -10;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean isGranted() {
        return granted;
    }

    public void setGranted(boolean granted) {
        this.granted = granted;
    }
}