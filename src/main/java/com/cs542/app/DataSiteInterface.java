/**
 *
 */
package com.cs542.app;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.ArrayList;

public interface DataSiteInterface extends Remote {
    public void unblock() throws RemoteException;

    public void abort() throws RemoteException;

    public void executeOperation(Operation operation) throws RemoteException;

    public void transactionDone(TransactionId txnId) throws RemoteException;

    public void hasFinished(int id) throws RemoteException;

    public void abortTxn(int abortedIndex) throws RemoteException;


}
