package com.cs542.app;

import ch.petikoch.libs.jtwfg.*;
import org.javatuples.Pair;

import java.util.*;
import java.util.logging.Logger;

public class LockManager {
    //    private Logger LOG;
//    static {
//        System.setProperty("java.util.logging.SimpleFormatter.format",
//                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
//        LOG = Logger.getLogger(LockManager.class.getName());
//    }
    // maps key to lock
    private HashMap<String, List<Lock>> lockTable;

    private static final Logger LOG = Logger.getLogger(LockManager.class.getName());

    // maps key to list of ops waiting
    private HashMap<String, List<Operation>> opQueue;

    private CentralSite cs;

    public LockManager(CentralSite cs) {
        this.cs = cs;
        lockTable = new HashMap<>();
        opQueue = new HashMap<>();
    }

    public boolean requestLock(Operation op) throws Exception {
        if (isCompatible(op)) {
            Lock prevLock = findPrevTransactionLock(op.getTransactionId(), op.getKey());
//            System.out.println("prevLock:" + prevLock);
            if (prevLock != null) {
                prevLock.upgradeType(op.getLockType());
            } else {
                Lock lock = createLockForOp(op);
                addLockForOp(lock);
                LOG.info("[LockManager] created lock for " + op + " " + lock);
//                lockTable.putIfAbsent(lock.getKey(), new ArrayList<>());
//                lockTable.get(lock.getKey()).add(getLockFromOp(op));
            }
            return true;
        } else {
            Lock lock = createLockForOp(op);
            LOG.info("Op " + op + " wait in queue, created lock:" + lock);
            opQueue.putIfAbsent(lock.getKey(), new ArrayList<>());
            opQueue.get(lock.getKey()).add(op);
            return false;
        }
    }

    /* abort a transaction and returns list of unblocked sites, only current usage is resolving deadlock  */
    public List<Integer> abortTransaction(TransactionId transactionId) {
        try {
            removeOps(transactionId);
            ArrayList<Integer> sites = releaseAndGrant(transactionId);
            LOG.info("Aborting: " + transactionId.toString());
            LOG.info("List of sites to be unblocked: " + sites.toString());
            return sites;
        } catch (Exception e) {
            LOG.warning("Error aborting transaction " + transactionId.toString() + ": " + e.getMessage());
            return new ArrayList<>();
        }
    }

    private void removeOps(TransactionId id) {
        // for each operation waiting for each key, if the operation belongs to this transaction, remove it
        for (Map.Entry<String, List<Operation>> waiting : opQueue.entrySet()) {
            waiting.getValue().removeIf(op -> op.getTransactionId().equals(id));
        }
        printTables();
    }

    public ArrayList<Lock> getTransactionLocks(TransactionId id) {
        ArrayList<Lock> locks = new ArrayList<Lock>();
        lockTable.forEach((key, lcks) -> {
            lcks.forEach(lck -> {
                if (lck.transaction.equals(id)) {
                    locks.add(lck);
                }
            });
        });
//        System.out.println("getTransactionLocks:" + locks.toString());
        return locks;
    }

    public Lock findPrevTransactionLock(TransactionId transactionId, String key) {
        if (lockTable.containsKey(key)) {
            List<Lock> tempList = lockTable.get(key);
            for (Lock lck : tempList) {
                if (lck.transaction.equals(transactionId)) {
                    return lck;
                }
            }
        }
        return null;
    }

    /* remove from locktable and grant next operation */
    public ArrayList<Integer> releaseAndGrant(TransactionId id) {
        ArrayList<Integer> sites = new ArrayList<>();
        ArrayList<Lock> heldLocks = getTransactionLocks(id);
//        System.out.println(heldLocks.toString());
        heldLocks.forEach(lock -> {
            lockTable.get(lock.getKey()).removeIf(l -> l.equals(lock)); // remove from table
            Operation op = dequeueOp(lock.getKey()); // get next op waiting if compatible
            LOG.info("[LockManager] dequeueOp:" + op);

            if (op != null) {
                Lock prevLock = findPrevTransactionLock(op.getTransactionId(), lock.getKey());
                if (prevLock != null) {
                    // if held a read lock before and need write lock, can upgrade
                    prevLock.upgradeType(op.getLockType());
                } else {
//                    lockTable.putIfAbsent(lock.getKey(), new ArrayList<>());
//                    lockTable.get(lock.getKey()).add(getLockFromOp(op));
                    Lock newlock = createLockForOp(op);
                    addLockForOp(newlock);
                }
                sites.add(op.transactionId.getSiteId());
            }
        });
        LOG.info("Transaction: " + id.toString() + " released locks");
        printTables();
//        System.out.println(sites.toString());
        return sites;
    }

    public Operation dequeueOp(String key) {
        if (!opQueue.containsKey(key)) {
            return null;
        }
        Operation res = null;
        List<Operation> ops = opQueue.get(key);
        if (isCompatible(ops.get(0))) {
            Operation operation = ops.remove(0);
            if (ops.isEmpty()) {
                opQueue.remove(key);
            }
            res = operation;
        }
        return res;
    }

    public Pair<TransactionId, List<Integer>> checkDeadlocks() {
        printTables();
        Map<String, TransactionId> tempMap = new HashMap<>();

        GraphBuilder<String> builder = new GraphBuilder<>();
        // op(0) -> lock(0), op(1) -> lock(1)
//        builder.addTaskWaitsFor("A", "B");
//        builder.addTaskWaitsFor("B", "A");
        lockTable.forEach((key, locks) -> {
            List<Operation> blocked = opQueue.get(key);
            if (blocked != null && !blocked.isEmpty()) {
                locks.forEach(lock -> {
                    blocked.forEach(b -> {
                        if (!lock.transaction.equals(b.transactionId)) {
                            String temp1 = b.getTransactionId().getSiteId() + "+" + b.getTransactionId().getSeqNum();
                            String temp2 = lock.transaction.getSiteId() + "+" + lock.transaction.getSeqNum();
                            tempMap.put(temp1, b.getTransactionId());
                            tempMap.put(temp2, lock.transaction);
                            builder.addTaskWaitsFor(temp1, temp2);
//                            builder.addTaskWaitsFor(b.transactionId, lock.transaction);
//                            System.out.println(b.transactionId + " " + lock.transaction);
                        }
                    });
                });
            }
        });

        Graph<String> waitForGraph = builder.build();
        DeadlockDetector<String> deadlockDetector = new DeadlockDetector<>();
        DeadlockAnalysisResult<String> analysisResult = deadlockDetector.analyze(waitForGraph);
        if (analysisResult.hasDeadlock()) {
            DeadlockCycle<String> cycle = analysisResult.getDeadlockCycles().iterator().next();
            List<String> txns = cycle.getCycleTasks();
//            System.out.println(txns.toString());
            TransactionId temp1 = tempMap.get(txns.get(0));
            TransactionId temp2 = tempMap.get(txns.get(1));
            LOG.info("deadlocked transactions: " + temp1 + " " + temp2);
            LOG.info("aborting txn: " + temp2);
            List<Integer> unblocked = abortTransaction(temp2);
            return new Pair<TransactionId, List<Integer>>(temp2, unblocked);
        }

        return null; // no deadlock
    }

    public Lock createLockForOp(Operation op) {
        Lock newLock = new Lock(op.getLockType(), op.getKey(), op.getTransactionId());
        return newLock;
    }

    public void addLockForOp(Lock lock) {
        lockTable.putIfAbsent(lock.getKey(), new ArrayList<>());
        lockTable.get(lock.getKey()).add(lock);
    }

    public Lock getLockFromOp(Operation op) {
        List<Lock> locks = lockTable.get(op.getKey());
        if (locks == null)
            return null;
        for (Lock lock : locks) {
            if (lock.getTransaction().equals(op.getTransactionId())) {
                return lock;
            }
        }
        return null;
    }

    public boolean isCompatible(Operation op) {
        if (!lockTable.containsKey(op.getKey()))
            return true;
        for (Lock lck : lockTable.get(op.getKey())) {
            if (op.getLockType() == LockType.READ) {
                if (!lck.getTransaction().equals(op.getTransactionId()) &&
                        lck.getType() == LockType.WRITE) {
                    return false;
                }
            } else {
//                System.out.println("l:" + lck.getTransaction());
//                System.out.println("lo:" + op.getTransactionId());
//                System.out.println(lck.getTransaction().equals(op.getTransactionId()));
                if (!lck.getTransaction().equals(op.getTransactionId())) {
                    return false;
                }
            }
        }
        return true;
    }

    public void printTables() {
//        LOG.info("------------Locktable------------");
//        lockTable.forEach((key, lcks) -> {
//            LOG.info(lcks.toString());
//        });
//        LOG.info("-------------OpQueue-------------");
//        opQueue.forEach((key, ops) -> {
//            LOG.info(ops.toString());
//        });
//        LOG.info("---------------------------------");
    }
}

