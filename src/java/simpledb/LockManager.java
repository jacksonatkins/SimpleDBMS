package simpledb;

import java.util.*;
/**
 * As described in HW3 Section 2.4
 */
public class LockManager {

    public class LocksOnPage {

        private TransactionId exclusiveLock;
        private Set<TransactionId> sharedLocks;

        public LocksOnPage() {
            this.sharedLocks = new HashSet<>();
            this.exclusiveLock = null;
        }

        public void addSharedLock(TransactionId tid) {
            this.sharedLocks.add(tid);
        }

        public void removeSharedLock(TransactionId tid) {
            this.sharedLocks.remove(tid);
        }

        public void removeExclusiveLock(TransactionId tid) {
            if (tid.equals(exclusiveLock)) {
                exclusiveLock = null;
            }
        }

        public void setExclusiveLock(TransactionId tid) {
            this.exclusiveLock = tid;
        }

        public boolean exclusivelyLocked() {
            return this.exclusiveLock != null;
        }

        public boolean holdsSharedLock(TransactionId tid) {
            return this.sharedLocks.contains(tid);
        }

        public boolean holdsExclusiveLock(TransactionId tid) {
            if (this.exclusiveLock != null) {
                return this.exclusiveLock.equals(tid);
            }
            return false;
        }

        public boolean isLocked() {
            return this.exclusiveLock != null || this.sharedLocks.size() != 0;
        }

    }

    private Map<PageId, LocksOnPage> locks;
    private Map<TransactionId, HashSet<TransactionId>> dependencies;

    public LockManager() {
        this.locks = new HashMap<>();
        this.dependencies = new HashMap<>();
    }

    // Checks if there are any deadlocks relating to transaction tid
    public synchronized boolean deadlocked(TransactionId tid) {
        if (this.dependencies.keySet().size() <= 1) {
            return false;
        }
        if (this.dependencies.containsKey(tid)) {
            Map<TransactionId, Integer> visited = new HashMap<>();
            LinkedList<TransactionId> queue = new LinkedList<>();
            visited.put(tid, 1);
            queue.add(tid);

            while (queue.size() != 0) {
                TransactionId s = queue.poll();
                if (!visited.containsKey(s)) {
                    visited.put(s, 1);
                    if (this.dependencies.containsKey(s)) {
                        queue.addAll(this.dependencies.get(s));
                    }
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    // Acquires a lock for Transaction tid on page with PageId pid.
    // Uses perms to determine if the lock is exclusive or shared.
    public void acquire(TransactionId tid, PageId pid, Permissions perms) throws TransactionAbortedException {
        if (this.locks.containsKey(pid) && this.locks.get(pid).holdsExclusiveLock(tid)) {
            return;
        }

        synchronized (this) {
            if (isLocked(pid)) {
                if (this.locks.get(pid).exclusivelyLocked() && !this.locks.get(pid).exclusiveLock.equals(tid)) {
                    if (!this.dependencies.containsKey(this.locks.get(pid).exclusiveLock)) {
                        this.dependencies.put(this.locks.get(pid).exclusiveLock, new HashSet<>());
                    }
                    this.dependencies.get(this.locks.get(pid).exclusiveLock).add(tid);
                } else if (this.locks.get(pid).sharedLocks.size() > 0 && perms.equals(Permissions.READ_WRITE)) {
                    for (TransactionId t : this.locks.get(pid).sharedLocks) {
                        if (!t.equals(tid)) {
                            if (!this.dependencies.containsKey(t)) {
                                this.dependencies.put(t, new HashSet<>());
                            }
                            this.dependencies.get(t).add(tid);
                        }
                    }
                }
            }
        }

        while (true) {
            synchronized (this) {
                if (deadlocked(tid)) {
                    throw new TransactionAbortedException();
                }
                if (!this.locks.containsKey(pid)) {
                    this.locks.put(pid, new LocksOnPage());
                    if (perms.equals(Permissions.READ_ONLY)) {
                        this.locks.get(pid).addSharedLock(tid);
                    } else {
                        this.locks.get(pid).setExclusiveLock(tid);
                    }
                    return;
                } else if (!this.locks.get(pid).exclusivelyLocked()) {
                    if (perms.equals(Permissions.READ_ONLY)) {
                        this.locks.get(pid).addSharedLock(tid);
                        return;
                    }
                    if (this.locks.get(pid).holdsSharedLock(tid) && this.locks.get(pid).sharedLocks.size() == 1) {
                        this.locks.get(pid).removeSharedLock(tid);
                        this.locks.get(pid).setExclusiveLock(tid);
                        return;
                    }
                } else {
                    if (this.locks.get(pid).exclusivelyLocked() && this.locks.get(pid).exclusiveLock.equals(tid)) {
                        return;
                    }
                }
            }
        }

    }

    // Releases the lock on page pid held by transaction tid.
    public synchronized void release(TransactionId tid, PageId pid) {
        if (this.isLocked(pid)) {
            if (this.locks.get(pid).holdsSharedLock(tid)) {
                this.locks.get(pid).removeSharedLock(tid);
            }
            if (this.locks.get(pid).holdsExclusiveLock(tid)) {
                this.locks.get(pid).removeExclusiveLock(tid);
            }
            // Maybe we don't need this check?
            if (this.locks.get(pid).sharedLocks.size() == 0 && !this.locks.get(pid).exclusivelyLocked()) {
                this.locks.remove(pid);
            }
        }
    }

    // Returns true if any transaction holds a lock on the page pid
    public synchronized boolean isLocked(PageId pid) {
        return this.locks.containsKey(pid) && this.locks.get(pid).isLocked();
    }

    // Returns true if the transaction tid holds a lock on the page pid
    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        return this.locks.get(pid).holdsExclusiveLock(tid) || this.locks.get(pid).holdsSharedLock(tid);
    }

    // Removes all held locks held by tid
    public synchronized void removeAllHeld(TransactionId tid) {
        List<PageId> toRelease = new ArrayList<>();
        for (PageId pid : this.locks.keySet()) {
            if (this.locks.get(pid).holdsExclusiveLock(tid) || this.locks.get(pid).holdsSharedLock(tid)) {
                toRelease.add(pid);
            }
        }
        for (PageId pid : toRelease) {
            this.release(tid, pid);
        }
        this.dependencies.remove(tid);
    }

    // Used to reset the lock manager.
    // Done by removing all locks stored in the manager.
    public void reset() {
        this.locks.clear();
        this.dependencies.clear();
    }

}
