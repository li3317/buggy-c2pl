package com.cs542.app;

import org.javatuples.Pair;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.logging.Logger;

public class CentralSite extends UnicastRemoteObject implements CentralSiteInterface, Runnable {
//    private Logger LOG;
//    static {
//        System.setProperty("java.util.logging.SimpleFormatter.format",
//                "[%1$tF %1$tT] [%4$-7s]  %5$s %n"); // TODO: [Central]
//        LOG = Logger.getLogger(CentralSite.class.getName());
//
//    }
    private int port;
    private LockManager lm;
    private int siteCount = 0;
    private Map<TransactionId, Integer> txnCounter;
    private int counter;

    private Set<Integer> finishedSites;

    Map<Integer, Integer> ports;

    // for printing summary
    private int numDeadlocks;
    public static int DELAY = 500;
    public int totalSites;


    private static final Logger LOG = Logger.getLogger(CentralSite.class.getName());

    public CentralSite(int port, String url, int totalSites) throws RemoteException {
        super();


        this.totalSites = totalSites;
        lm = new LockManager(this);
        this.counter = 0;
        txnCounter = new HashMap<>();
        this.port = port;
        finishedSites = new HashSet<>();

        try {
            LocateRegistry.createRegistry(port);
            Naming.rebind("//" + url + ":" + port + "/central", this);

            // TODO: this log is useless!! fix!!
            this.LOG.info(this.getClass().getName());

            Timer timer = new Timer();
            this.LOG.info("Starting timer for checking deadlock");
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
//                    CentralSite.printRegistry(port);
                    checkDeadlocks();
                }
            }, DELAY, DELAY);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        System.out.println("central exit?");
    }

    public Map<Integer, Integer> getPorts() {
        return ports;
    }

    public void setPorts(Map<Integer, Integer> ports) {
        this.ports = ports;
    }

    @Override
    public int setTxnCounter(TransactionId txn) throws RemoteException {
        System.out.println(txn + " in map " + txnCounter.toString() + " ? " + txnCounter.containsKey(txn));
        if (txnCounter.containsKey(txn)) {
            return txnCounter.get(txn);
        } else {
            LOG.info("Central site received new txn request, counter: " + counter);
            int res = counter;
            txnCounter.put(txn, counter);
            counter++;
            return res;
        }
    }

    // when receiving a request for lock, assign timestamp (counter) to the transaction this operation belongs to
    // operation from same transaction will arrive in order, and updates made to preCommit table will be
    // sorted based on transaction index then operation index

    @Override
    public synchronized void finishSite(int siteId) throws RemoteException {
        finishedSites.add(siteId);
        if (finishedSites.size() == totalSites) {
            LOG.info("Central: all sites finished, exiting");
            // central site exit
        }
    }

    public void printRegistry(int port) {
        // don't know what this does yet, keeping it for now
        try {
            Registry registry = LocateRegistry.getRegistry(port);
            String[] list = registry.list();
            int n = list.length;
            StringBuilder builder = new StringBuilder();
            builder.append("RMI Registry: <");
            for(int i = 0; i < n; i += 1) {
                if(i == 0) {
                    builder.append(list[i]);
                }
                else {
                    builder.append(", ").append(list[i]);
                }
            }
            builder.append(">");
            LOG.info(builder.toString());
        }
        catch(RemoteException e) {
            LOG.info(" Remote Exception: " + e.getMessage());
        }
    }

    public synchronized void checkDeadlocks() {
//        LOG.info("Checking Deadlocks...");
        Pair<TransactionId, List<Integer>> result = lm.checkDeadlocks();
        if(result != null) {
            numDeadlocks++;
            TransactionId abortTxn = result.getValue0();
            LOG.info("Deadlock detected, aborting " + abortTxn.toString());
            try {
                DataSiteInterface dataSite = getDataSite(abortTxn.getSiteId());
                if (dataSite == null) {
                    LOG.info("Datasite " + abortTxn.getSiteId() + " is null!");
                }
                else {
                    // aborting txn dataSite is working on, remove it from txnCounter map
                    System.out.println("abort: txn " + abortTxn + " in map " + txnCounter.toString() + " ? " + txnCounter.containsKey(abortTxn));
                    txnCounter.remove(abortTxn);

                    dataSite.abort();
                    LOG.info("Site " + abortTxn.getSiteId() + " aborted " + abortTxn.toString());
                }
            }
            catch(Exception e) {
                LOG.info("Exception in getting data site: " + e.getMessage());
                e.printStackTrace();
            }

            List<Integer> unblockedSites = result.getValue1();
            LOG.info("Unblocking sites: " + unblockedSites.toString());
            unblockedSites.forEach(site -> {
                try {
                    DataSiteInterface dataSite = getDataSite(site);
                    LOG.info("Got site " + site);
                    if (dataSite != null) {
                        dataSite.unblock();
                        LOG.info("Data Site " + site + " unblocked");
                    }
                } catch (Exception e) {
                    LOG.info("Exception occurred when unblocking: " + e.getMessage());
                    e.printStackTrace();
                }
            });
        }
    }

    private DataSiteInterface getDataSite(int siteId) {
        LOG.info("Looking up datasite " + siteId);
        try {
            LOG.info("[central] looking up: " + "//localhost:" + ports.get(siteId) + "/ds" + siteId);
            DataSiteInterface ds = (DataSiteInterface) Naming.lookup("//localhost:" + ports.get(siteId) + "/ds" + siteId);
            return ds;
        }
        catch (RemoteException e) {
            LOG.info("Remote Exception: " + e.getMessage());
            e.printStackTrace();
        }
        catch (NotBoundException e) {
            LOG.info("Not Bound Exception: " + e.getMessage());
            e.printStackTrace();
        }
        catch (NullPointerException e) {
            LOG.info("Null Pointer Exception: " + e.getMessage());
            e.printStackTrace();
        }
        catch (MalformedURLException e) {
            LOG.info("Malformed URL Exception: " + e.getMessage());
            e.printStackTrace();
        } catch(Exception e) {
            LOG.info("Other exception in getDataSiteStub: " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    public synchronized void releaseLock(TransactionId txn)  throws RemoteException {
        LOG.info("Transaction: " + txn.toString() + " wants to release locks");
        try {
            List<Integer> unblockedSites = lm.releaseAndGrant(txn);
            LOG.info("Sites unblocked after release: " + unblockedSites.toString());

            unblockedSites.forEach(site -> {
                DataSiteInterface dataSite = getDataSite(site);
                if (dataSite != null) {
                    try {
                        dataSite.unblock();
                    } catch (RemoteException e) {
                        e.printStackTrace();
                    }
                } else {
                    LOG.info("Datasite from release" + site + " null!");
                }
            });
        }
        catch (NullPointerException e) {
            LOG.info("Null Pointer Exception in releaseLock: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            LOG.info("Exception in releaseLock: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public synchronized RequestLockResult requestLock(Operation op)  throws RemoteException {
        LOG.info("Operation: " + op.toString() + " " + op.hashCode() + " requests locks");
        RequestLockResult result = new RequestLockResult();
        try {
            boolean granted = lm.requestLock(op);
            if (granted) {
                // assign counter to transaction
                TransactionId txn = op.transactionId;
                if (!txn.isIndexSet()) {
//                    if (txnCounter.containsKey(txn)) {
////                        txn.setIndex(txnCounter.get(txn));
//                        result.setIndex(txnCounter.get(txn));
//                    } else {
//                        LOG.info("Central site received new txn request, counter: " + counter);
////                        op.transactionId.setIndex(counter);
//                        result.setIndex(counter);
//                        txnCounter.put(txn, counter);
//                        counter++;
//                    }
                    int setCounter = this.setTxnCounter(txn);
                    System.out.println("central: lock granted, index: " + setCounter);
                    result.setIndex(setCounter);
                } else {
                    result.setIndex(txn.getIndex());
                    LOG.info("Central site received op " + op.index + " of txn " + op.transactionId.toString());
                }
            }
            result.setGranted(granted);
            return result;
        }
        catch(NullPointerException e) {
            LOG.info("Null Pointer Exception in requestLock: " + e.getMessage());
            e.printStackTrace();
        }
        catch(Exception e) {
            LOG.info("Exception in requestLock: " + e.getMessage());
            e.printStackTrace();
        }
        // not granted due to error
        result.setGranted(false);
        return result;
    }

    public synchronized Set<Integer> getFinishedSites() {
        return finishedSites;
    }

    @Override
    public void run() {
        while (getFinishedSites().size() < totalSites) {
//            System.out.println("central running");
            synchronized (this) {
                try {
                    wait(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
