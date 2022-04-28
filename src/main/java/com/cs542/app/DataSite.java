/**
 * 
 */
package com.cs542.app;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.*;
import java.util.ArrayList;
import java.rmi.*;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class DataSite extends UnicastRemoteObject implements Runnable, DataSiteInterface {

	private TransactionManager tm;
	public int id;
	private CentralSiteInterface cs;
	private List<DataSiteInterface> participants;

	public long startTime;

	// dataSite states
	private boolean isBlocked;
	private boolean isAbort;

//	private static Logger LOG = null;
//	static {
//		System.setProperty("java.util.logging.SimpleFormatter.format",
//				"[%1$tF %1$tT] [%4$-7s] %5$s %n");
//		LOG = Logger.getLogger(DataSite.class.getName());
//	}
	private static final Logger LOG = Logger.getLogger(DataSite.class.getName());
//	private final String TXNFILE = "transaction/transaction1.txt";

	public DataSite(int id, int port, String url, String txnFile, int centralPort, int totalSites) throws RemoteException, FileNotFoundException, UnsupportedEncodingException {
		super();
		isBlocked = false;
		isAbort = false;
		this.id = id;
		tm = new TransactionManager(this.id, totalSites, this);
		participants = new ArrayList<>();
		try {
			// set up dataSite
			LocateRegistry.createRegistry(port);
			String name = "//" + url + ":" + port + "/ds" + id;
			Naming.rebind(name, this);
			LOG.info("[" + id + "] Bind name: " + name);

			// set up connection to central site
			LOG.info("[" + id + "] looking up1: " + "//" + url + ":" + centralPort + "/central");
			cs = (CentralSiteInterface) Naming.lookup("//" + url + ":" + centralPort + "/central");

			// load transactions from file
			tm.loadTxnFile(txnFile);

			System.out.println("here2");

		} catch(RemoteException e) {
			LOG.warning("[" + id + "] Remote Exception: " + e.getMessage());
		} catch(NotBoundException e) {
			LOG.warning("[" + id + "] Not Bound Exception: " + e.getMessage());
			e.printStackTrace();
		} catch(MalformedURLException e) {
			LOG.warning("[" + id + "] Malformed URL exception: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			LOG.warning("[" + id + "] Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}

	public void setUpParticipants(String url, Map<Integer, Integer> ports, int totalSites) {
		try {
			for (int i = 1; i <= totalSites; i++) {
				if (i != id) {
					LOG.info("[" + id + "] looking up2: " + "//" + url + ":" + ports.get(i) + "/ds" + i);
					DataSiteInterface temp = (DataSiteInterface)
							Naming.lookup("//" + url + ":" + ports.get(i) + "/ds" + i);
					participants.add(temp);
				}
			}
		} catch(RemoteException e) {
			LOG.warning("[" + id + "] Remote Exception: " + e.getMessage());
		} catch(NotBoundException e) {
			LOG.warning("[" + id + "] Not Bound Exception: " + e.getMessage());
		} catch(MalformedURLException e) {
			LOG.warning("[" + id + "] Malformed URL exception: " + e.getMessage());
			e.printStackTrace();
		} catch (Exception e) {
			LOG.warning("[" + id + "] Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// this will be invoked by TMs on other sites to add their operation into preCommitTab
	// TODO: removed a synchronized keyword ? clearly dont know how synchronization work in java
	@Override
	public void executeOperation(Operation operation) throws RemoteException {
		try {
			System.out.println("[" + id + "] exe op pe:" + operation.getTransactionId());
			tm.executeOperation(operation);
		} catch (Exception e) {
			LOG.warning("[" + id + "] exception in executeOperation, op not executed!");
			e.printStackTrace();
		}
	}

	@Override
	public void transactionDone(TransactionId txnId) throws RemoteException {
		tm.commit(txnId.getIndex());
		LOG.info("[" + id + "] Added " + txnId.toString() + " to waitingTxn");
	}

	@Override
	public void abort() throws RemoteException {
		isAbort = true;
		LOG.info("[" + id + "] Aborted transaction");
		unblock();
		//notifyAll();

		// major design problem:
		// after unblocking, the current transaction index is no longer valid and commit worker on any site
		// shouldn't wait for it , so after abort, send a message to each site to send a message (like finishSite)
		// to their commit worker and skip the transaction
		// then, add the transaction back to txnHistory so it can request lock again

	}

	@Override
	public void abortTxn(int abortedIndex) throws RemoteException {
		tm.abortTxn(abortedIndex);
	}

	@Override
	public void unblock() throws RemoteException {
		LOG.info("[" + id + "] Unblocking site");
		isBlocked = false;
//		notifyAll();
	}

	public void blocked() {
		try {
			isBlocked = true;
			while(isBlocked) {
				LOG.info("[" + id + "] Site " + id + " blocked. Waiting...");
				wait(500);
				// TODO: improve this, wake the thread up instead of waiting for a full second on lock release
			}

			// there are two scenerio a data site is unblocked:
			// 1. it got a lock its waiting for: in this case txnCounter needs to be set as it might be newly added to locktable
			// 2. it needs to abort this transaction, in this case transactionId index for the newly granted txn needs to be set
			//
		}
		catch(InterruptedException e) {
			LOG.info("[" + id + "] Interrupted Exception: " + e.getMessage());
		}
		catch(NullPointerException e) {
			LOG.info("[" + id + "] Null Pointer Exception: " + e.getMessage());
		}
		catch(Exception e) {
			LOG.info("[" + id + "] Exception: " + e.getMessage());
			e.printStackTrace();
		}
	}

	// this is called by other sites when they committed and send out all operations for all transactions
	@Override
	public void hasFinished(int id) { //synchronized
		tm.finishSite(id);
	}

	@Override
	public void run() {
		System.out.println("running site " + id);
		this.startTime = System.currentTimeMillis();
		while(true) {
			try {
				synchronized (this) {
//					LOG.info("[" + id + "] ???");
					Transaction transaction = tm.popTransaction();
					if (transaction != null) {
						LOG.info("[" + id + "] Starting transaction " + transaction.transactionId.toString());
//						System.out.println(transaction.getOperations().size());
						for (Operation operation : transaction.getOperations()) {
							switch(operation.getType()) {
								case GET:
								case PUT:
									System.out.println("request lock:" + operation + " " + operation.transactionId);
									RequestLockResult result = cs.requestLock(operation);
									if (result.isIndexSet()) {
										operation.transactionId.setIndex(result.index);
									}
									System.out.println("set1?" + transaction.getTransactionId().isIndexSet());
									if(!result.granted) {
										LOG.info("[" + id + "] lock for " + operation + " not granted");
										blocked();

										if (!isAbort) {
											// lock granted, obtain index first
											int newIndex = cs.setTxnCounter(transaction.getTransactionId());
											System.out.println("got index " + newIndex + " , set ? " + operation.transactionId.getIndex());
											if (!operation.transactionId.isIndexSet()) {
												operation.transactionId.setIndex(newIndex);
												System.out.println("set2?" + transaction.getTransactionId().isIndexSet());
											}
										}
									}
									if(isAbort) {
										break;
									}
									System.out.println("exe op:" + operation.getTransactionId().getIndex());
									tm.executeOperation(operation);
									break;

								case APPEND: // must read transaction before append
									System.out.println("append txn id :" + operation.getTransactionId());
									System.out.println("exe op:" + operation.getTransactionId().getIndex());
									tm.executeOperation(operation);
									break;
							}

							if(isAbort) {
								break;
							}

							System.out.println("added: " + operation);

							// this op is save to be added to preCommitTab, update other sites
							participants.forEach(p -> {
								try {
									System.out.println("exe op p:" + operation.getTransactionId().getIndex());
									p.executeOperation(operation);
								} catch (RemoteException e) {
									LOG.info("[" + id + "] Remote Exception: " + e.getMessage());
									e.printStackTrace();
								}
							});

							LOG.info("[" + id + "] done op: " + operation);
						}

						if(isAbort) {
							int abortedIndex = transaction.getTransactionId().getIndex();
							LOG.info("[" + id + "] Transaction " + transaction.transactionId.toString() + " aborted");
							tm.addTransaction(transaction, transaction.transactionId); // add it back into txnHistory
							this.abortTxn(abortedIndex);
							participants.forEach(p -> {
								try {
									p.abortTxn(abortedIndex);
								} catch (RemoteException e) {
									LOG.info("[" + id + "] Remote Exception: " + e.getMessage());
									e.printStackTrace();
								}
							});

							isAbort = false;
							System.out.println("sleep 1 sec");
							wait(500); // 1000
							System.out.println("wake up");
						} else {
							System.out.println(transaction.getTransactionId() + " -1");
							this.transactionDone(transaction.getTransactionId());
							participants.forEach(p -> {
								try {
									p.transactionDone(transaction.getTransactionId());
								} catch (RemoteException e) {
									LOG.info("[" + id + "] Remote Exception: " + e.getMessage());
									e.printStackTrace();
								}
							});
							// TODO: even if commit didn't go through and just data was added to preCommitTab, is it safe to release lock??
							cs.releaseLock(transaction.transactionId);
							LOG.info("[" + id + "] Transaction " + transaction.transactionId.toString() + " completed");
						}
					} else {
						LOG.info("[" + id + "] All txn done, finish site");
						// finished all txn on this site, notify all other sites including itself
						this.hasFinished(id);
						participants.forEach(p -> {
							try {
								p.hasFinished(this.id);
							} catch (RemoteException e) {
								LOG.info("[" + id + "] Remote Exception: " + e.getMessage());
							}
						});
						break;

					}
				}
			}
			catch(RemoteException e) {
				LOG.info("[" + id + "] Remote Exception: " + e.getMessage());
			}
			catch(Exception e) {
				LOG.info("[" + id + "] Exception: " + e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
