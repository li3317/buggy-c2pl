package com.cs542.app;

import java.io.*;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionManager {
//    private static Logger LOG = null;
    public static final int OP_DELAY = 100;

    private static final Logger LOG = Logger.getLogger(TransactionManager.class.getName());

//    static {
//        System.setProperty("java.util.logging.SimpleFormatter.format",
//                "[%1$tF %1$tT] [%4$-7s] %5$s %n");
//        LOG = Logger.getLogger(TransactionManager.class.getName());
//    }

    private DBManager dbMgr; // only used by CommitWorker
    private  int siteId;
    private int seqNum;
    private ConcurrentHashMap<Integer, List<Operation>> preCommitTab;
    public List<Transaction> txnHistory;
    public Set<Integer> waitingTxn;
    private Set<Integer> finishedSites;
    private Set<Integer> abortedTxn;
    private int txnCounter = 0;
    private int totalSites;

    private DataSite ds;

    private PrintWriter writer;

    public TransactionManager(int siteId, int totalSites, DataSite ds) throws FileNotFoundException, UnsupportedEncodingException {
        this.siteId = siteId;
        this.totalSites = totalSites;
        txnHistory = new ArrayList<>();
        preCommitTab = new ConcurrentHashMap<>();
        waitingTxn = ConcurrentHashMap.newKeySet();
        finishedSites = ConcurrentHashMap.newKeySet();
        abortedTxn = ConcurrentHashMap.newKeySet();
        writer = new PrintWriter("output/ds" + siteId + "-out.txt", "UTF-8");
        dbMgr = new DBManager(siteId, writer);
        this.ds = ds;
        setUpDB();

        ExecutorService taskList = Executors.newFixedThreadPool(1);

        taskList.execute(new CommitWorker());
        taskList.shutdown();



//        commitWorker = new CommitWorker();
//        System.out.println("commit worker");
//        commitWorker.run();
//        System.out.println("out");
    }

    public void setUpDB() {
        String dbn = "c2pl" + siteId;
        dbMgr.connDataBase("root", "password", dbn);
        LOG.info("[" + siteId + "] connected to " + dbn);
        dbMgr.createTable();
    }

    public synchronized DBManager getDbMgr() {
        return dbMgr;
    }

    public synchronized void setDbMgr(DBManager dbMgr) {
        this.dbMgr = dbMgr;
    }

    public synchronized Set<Integer> getFinishedSites() {
        return finishedSites;
    }

    // only commitThread can interact with database
    class CommitWorker implements Runnable {
        public void commitTxn(int index) throws Exception {
            System.out.println("[" + siteId + "] committing " + index);
            List<Operation> operations = preCommitTab.get(index);
            Map<String, String> preWriteTab = new HashMap<>();
            System.out.println("index: " + index + " " + operations);
            for (Operation operation : operations) {
                String key = operation.getKey();
                switch (operation.getType()) {
                    case GET:
                        String readVal = getDbMgr().read(key);
                        if (!readVal.isEmpty()) {
                            preWriteTab.put(key, readVal);
                        }
                        break;

                    case PUT:
                        System.out.println("put val:" + preWriteTab.get(key));
                        if(preWriteTab.containsKey(key)) {
                            getDbMgr().write(key, preWriteTab.get(key));
                        }
                        else {
                            throw new Exception("Write operation value not available!");
                        }
                        break;

                    case APPEND:
                        // Ops          db
                        // A(A, "a")    A -> a
                        // A(B, "b")    A -> a, B -> b
                        // A(A, "c")    A -> ac, B -> b
                        String appendRes;
                        if (preWriteTab.containsKey(key)) {
                            appendRes = preWriteTab.get(key) + operation.getValue();
                        } else {
                            appendRes = operation.getValue();
                        }
                        System.out.println("append val:" + appendRes);
                        preWriteTab.put(key, appendRes);
                        break;
                }
            }
            waitingTxn.remove(index);
            System.out.println("[" + siteId + "] remove " + index + " from waitingTxn");

        }

        @Override
        public void run() {
            while (true) {
                synchronized (this) {
                    while (!waitingTxn.contains(txnCounter)) {
                        System.out.println("cw:" + txnCounter + " " + siteId); // TODO: why does this keep printing after printing result???
                        if (abortedTxn.contains(txnCounter)) {
                            LOG.info("[" + siteId + "] (cw) skipping " + txnCounter + " waiting for " + (txnCounter + 1));
                            txnCounter++;
                        } // wait for the next round for new txnCounter
                        try {
//                        Thread.sleep(100);
                            wait(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    System.out.println("[" + siteId + "] found " + txnCounter);
                    try {
                        commitTxn(txnCounter);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    txnCounter++;
                    LOG.info("[cw " + siteId + "] next txn:" + txnCounter);
                    if (getFinishedSites().size() == totalSites && waitingTxn.isEmpty()) {
                        break;
                    }
                }
            }
            System.out.println("-------" + siteId + "-------");
            writer.println("-------" + siteId + "-------");
            getDbMgr().showAll();
            System.out.println("-------" + siteId + "-------");
            writer.println("-------" + siteId + "-------");
            getDbMgr().disConn();
            long end = System.currentTimeMillis();
            writer.println("site " + siteId + " took " + (end - ds.startTime) + " ms");
            writer.close();

        }
    }

    public int getSiteId() {
        return siteId;
    }

    public Transaction popTransaction() {
        return (txnHistory.isEmpty()) ? null : txnHistory.remove(0);
    }

    public void addTransaction(Transaction txn, TransactionId txnId) {
        int lastSeqNum = txnHistory.isEmpty() ? -1 : txnHistory.get(txnHistory.size() - 1).getTransactionId().getSeqNum();
        TransactionId newTxnId = new TransactionId(lastSeqNum + 1, txnId.getSiteId());
        txn.setTransactionId(newTxnId);
        txn.getOperations().forEach(op -> {
            op.setTransactionId(newTxnId);
        });
        txnHistory.add(txn);
    }

    // called by this datasite to wake up commitThread
    public void commit(int index) {
        waitingTxn.add(index);
        LOG.info("[" + siteId + "] " + index + " added to waitingTxn");
//        this.notifyAll();
    }

    public void abortTxn(int index) {
        abortedTxn.add(index);
        preCommitTab.remove(index);
    }

    public void finishSite(int site) {
        System.out.println("[" + siteId + "] site " + site + " finished");
        getFinishedSites().add(site);
    }

    public void executeOperation(Operation operation) throws Exception {
        try {
            Thread.sleep(TransactionManager.OP_DELAY);
            System.out.println("[" + siteId + "] execute op index:" + operation);
            int index = operation.getTransactionId().getIndex();
            preCommitTab.putIfAbsent(index, new ArrayList<>());
            preCommitTab.get(index).add(operation);
            LOG.info("[" + siteId + "] " + operation.transactionId + " added to preCommitTab, size:"
                    + preCommitTab.get(index).size());
        }
        catch (InterruptedException e) {
            System.out.println("Interrupted Exception: " + e.getMessage());
            e.printStackTrace();
        }


    }

    public void loadTxnFile(String txnFile) throws Exception {
        List<String> ops = new ArrayList<>();
        try {
            BufferedReader bf = new BufferedReader(new FileReader(txnFile));
            String line;
            while((line = bf.readLine()) != null && !line.trim().isEmpty()) {
                ops.add(line);
            }
            bf.close();
        }
        catch(IOException e) {
            LOG.info("[" + this.siteId + "] read txn file failed: " + e.getMessage());
        }
        txnHistory = getTxnFromFile(ops);
        LOG.info("[" + this.siteId + "] transaction created " + txnHistory.size() + " transactions");
    }

    public List<Transaction> getTxnFromFile(List<String> ops) throws Exception {
        List<Transaction> transactions = new ArrayList<>();
        int n = ops.size();
        int opIndex = 0;
        Transaction current = null;
        TransactionId currentId = null;
        for (int i = 0; i < n; i++) {
            String cmd = ops.get(i);
            if(cmd.toLowerCase().contains("start")) {
                if (current != null) { // end current txn
                    transactions.add(current);
                }
                currentId = new TransactionId(this.seqNum, this.siteId);
                System.out.println("1- " + currentId.toString());
                current = new Transaction(currentId);
                opIndex = 0;
                this.seqNum++;
            } else if (cmd.toLowerCase().contains("wait")) {
                int waitTime = Integer.parseInt(cmd.toLowerCase().split(":")[1]);
                Thread.sleep(waitTime);
            } else {
                char opType = cmd.trim().charAt(0);
                switch(opType) {
                    case 'r': {
                        Pattern pattern = Pattern.compile(".*\\((.*)\\).*");
                        Matcher matcher = pattern.matcher(cmd);
                        if(matcher.matches()) {
                            String key = matcher.group(1);
                            Operation operation = new Operation(key, null, OpType.GET, currentId, opIndex);
                            current.addOperation(operation);
                        }
                        else {
                            throw new Exception("Read operation unsupported format: " + cmd);
                        }
                        break;
                    }
                    case 'w': {
                        Pattern pattern = Pattern.compile(".*\\((.*)\\).*");
                        Matcher matcher = pattern.matcher(cmd);
                        if(matcher.matches()) {
                            String key = matcher.group(1);
                            Operation operation = new Operation(key, null, OpType.PUT, currentId, opIndex);
                            current.addOperation(operation);
                        } else {
                            throw new Exception("Write operation unsupported format: " + cmd);
                        }
                        break;
                    }
                    case 'a': { // append operation read key and append value to the result
                        Pattern pattern = Pattern.compile(".*\\((.*),(.*)\\).*");
                        Matcher matcher = pattern.matcher(cmd);
                        if(matcher.matches()) {
                            String key = matcher.group(1);
                            String value = matcher.group(2);
                            Operation operation = new Operation(key, value, OpType.APPEND, currentId, opIndex);
                            current.addOperation(operation);
                        } else {
                            throw new Exception("Append operation unsupported format: " + cmd);
                        }
                        break;
                    }
                    default:
                        throw new Exception("Undefined operation " + cmd);
                }
                opIndex++;
            }
        }
        if(current != null) {
            transactions.add(current);
        }
        return transactions;
    }

}
