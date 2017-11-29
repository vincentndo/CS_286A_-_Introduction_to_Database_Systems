import com.sun.org.apache.regexp.internal.RE;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<String, TableLock>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {

        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }

        if (!this.tableToTableLock.containsKey(tableName)) {
            TableLock tableLock = new TableLock(lockType);
            tableLock.lockOwners.add(transaction);
            this.tableToTableLock.put(tableName, tableLock);
            return;
        }

        HashSet<Transaction> lockOwnerSet = this.tableToTableLock.get(tableName).lockOwners;
        LockType tableLockType = this.tableToTableLock.get(tableName).lockType;
        LinkedList<Request> waitingQueue = this.tableToTableLock.get(tableName).requestersQueue;

        if (lockOwnerSet.contains(transaction) &&
                tableLockType == LockType.Exclusive && lockType == LockType.Shared) {
            throw new IllegalArgumentException();

        } else if (holds(transaction, tableName, lockType)) {
            throw new IllegalArgumentException();
        }

        if (compatible(tableName, transaction, lockType)) {
            lockOwnerSet.add(transaction);
            this.tableToTableLock.get(tableName).lockType = lockType;

        } else {

            if (this.deadlockAvoidanceType == DeadlockAvoidanceType.WaitDie) {
                waitDie(tableName, transaction, lockType);
                return;
            } else if (this.deadlockAvoidanceType == DeadlockAvoidanceType.WoundWait) {
                woundWait(tableName, transaction, lockType);
                return;
            }

            transaction.sleep();
            Request lockRequestingXact = new Request(transaction, lockType);

            if (lockOwnerSet.contains(transaction) &&
                    tableLockType == LockType.Shared && lockType == LockType.Exclusive) {
                waitingQueue.addFirst(lockRequestingXact);
            } else {
                waitingQueue.add(lockRequestingXact);
            }
        }
    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) {

        if (!this.tableToTableLock.containsKey(tableName)) {
            return true;
        } else if (this.tableToTableLock.get(tableName).lockOwners.isEmpty()) {
            return true;
        }

        HashSet<Transaction> lockOwnerSet = this.tableToTableLock.get(tableName).lockOwners;
        LockType tableLockType = this.tableToTableLock.get(tableName).lockType;

        if (!lockOwnerSet.contains(transaction) && tableLockType == LockType.Exclusive) {
            return false;
        }

        if (!lockOwnerSet.contains(transaction) &&
                tableLockType == LockType.Shared && lockType == LockType.Shared) {
            return true;
        }

        if (!lockOwnerSet.contains(transaction) &&
                tableLockType == LockType.Shared && lockType == LockType.Exclusive) {
            return false;
        }

        return (lockOwnerSet.contains(transaction) && lockOwnerSet.size() == 1 &&
                tableLockType == LockType.Shared && lockType == LockType.Exclusive);
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException{

        if (transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }

        if (!this.tableToTableLock.containsKey(tableName)) {
            throw new IllegalArgumentException();
        }

        HashSet<Transaction> lockOwnerSet = this.tableToTableLock.get(tableName).lockOwners;

        if (!lockOwnerSet.contains(transaction)) {
            throw new IllegalArgumentException();
        }

        lockOwnerSet.remove(transaction);

        Request firstRequester = this.tableToTableLock.get(tableName).requestersQueue.peek();

        if (firstRequester != null) {

            if (lockOwnerSet.size() == 1 &&
                    lockOwnerSet.contains(firstRequester.transaction) &&
                    firstRequester.lockType == LockType.Exclusive) {

                firstRequester.transaction.wake();
                this.tableToTableLock.get(tableName).lockType = LockType.Exclusive;
                this.tableToTableLock.get(tableName).requestersQueue.removeFirst();

            } else if (lockOwnerSet.size() > 1) {
                LinkedList<Request> waitingList = this.tableToTableLock.get(tableName).requestersQueue;

                for (Iterator<Request> it = waitingList.iterator(); it.hasNext(); ) {
                    Request requester = it.next();

                    if (requester.lockType == LockType.Shared) {
                        it.remove();
                        requester.transaction.wake();
                        lockOwnerSet.add(requester.transaction);
                    }
                }

            } else if (lockOwnerSet.size() == 0) {
                LinkedList<Request> waitingList = this.tableToTableLock.get(tableName).requestersQueue;

                if (firstRequester.lockType == LockType.Shared) {

                    for (Iterator<Request> it = waitingList.iterator(); it.hasNext(); ) {

                        Request requester = it.next();
                        if (requester.lockType == LockType.Shared) {
                            it.remove();
                            requester.transaction.wake();
                            lockOwnerSet.add(requester.transaction);
                        }
                    }
                    this.tableToTableLock.get(tableName).lockType = LockType.Shared;

                } else if (firstRequester.lockType == LockType.Exclusive) {
                    waitingList.removeFirst();
                    firstRequester.transaction.wake();
                    lockOwnerSet.add(firstRequester.transaction);
                    this.tableToTableLock.get(tableName).lockType = LockType.Exclusive;
                }
            }

        } else {
            if (lockOwnerSet.size() == 0) {
                this.tableToTableLock.remove(tableName);
            }
        }
    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
        if (!this.tableToTableLock.containsKey(tableName)) {
            return false;
        }

        HashSet<Transaction> lockOwnerSet = this.tableToTableLock.get(tableName).lockOwners;
        LockType tableLockType = this.tableToTableLock.get(tableName).lockType;
        return lockOwnerSet.contains(transaction) && tableLockType == lockType;
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
        if (!compatible(tableName, transaction, lockType)) {
            HashSet<Transaction> lockOwnerSet = this.tableToTableLock.get(tableName).lockOwners;
            for (Transaction xact : lockOwnerSet) {
                if (transaction.getTimestamp() > xact.getTimestamp()) {
                    transaction.abort();
                    return;
                }
            }
            transaction.sleep();
            Request requester = new Request(transaction, lockType);
            this.tableToTableLock.get(tableName).requestersQueue.add(requester);
        }
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
        if (!compatible(tableName, transaction, lockType)) {
            HashSet<Transaction> lockOwnerSet = this.tableToTableLock.get(tableName).lockOwners;
            for (Transaction xact : lockOwnerSet) {
                if (transaction.getTimestamp() >= xact.getTimestamp()) {
                    transaction.sleep();
                    Request requester = new Request(transaction, lockType);
                    this.tableToTableLock.get(tableName).requestersQueue.add(requester);
                    return;
                }
            }
            for (Transaction xact : lockOwnerSet) {
                xact.abort();
                for (Request requester : this.tableToTableLock.get(tableName).requestersQueue) {
                    if (xact == requester.transaction) {
                        this.tableToTableLock.get(tableName).requestersQueue.remove(requester);
                    }
                }
            }
            lockOwnerSet.clear();
            lockOwnerSet.add(transaction);
            this.tableToTableLock.get(tableName).lockType = lockType;
        }
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<Transaction>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}
