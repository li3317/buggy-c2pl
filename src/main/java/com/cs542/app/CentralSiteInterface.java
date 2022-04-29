package com.cs542.app;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface CentralSiteInterface extends Remote {
    public RequestLockResult requestLock(Operation operation) throws RemoteException;

    public void releaseLock(TransactionId txn) throws RemoteException;

    public void finishSite(int siteId) throws RemoteException;

    public int setTxnCounter(TransactionId txn) throws RemoteException;
}
