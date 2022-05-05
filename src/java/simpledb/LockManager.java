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

    public LockManager() {
        this.locks = new HashMap<>();
    }

    // Acquires a lock for Transaction tid on page with PageId pid.
    // Uses perms to determine if the lock is exclusive or shared.
    public void acquire(TransactionId tid, PageId pid, Permissions perms) throws InterruptedException {
        if (this.locks.containsKey(pid) && this.locks.get(pid).holdsExclusiveLock(tid)) {
            return;
        }

        while (true) {
            synchronized (this) {
                if (!this.locks.containsKey(pid)) {
                    this.locks.put(pid, new LocksOnPage());
                    if (perms.equals(Permissions.READ_ONLY)) {
                        this.locks.get(pid).addSharedLock(tid);
                    } else {
                        this.locks.get(pid).setExclusiveLock(tid);
                    }
                    return;
                } else if (!this.locks.get(pid).exclusivelyLocked() && perms.equals(Permissions.READ_ONLY)) {
                    this.locks.get(pid).addSharedLock(tid);
                    return;
                } else if (this.locks.get(pid).holdsSharedLock(tid) && this.locks.get(pid).sharedLocks.size() == 1) {
                    this.locks.get(pid).removeSharedLock(tid);
                    this.locks.get(pid).setExclusiveLock(tid);
                    return;
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
                this.locks.get(pid).setExclusiveLock(null);
            }
            this.locks.remove(pid);
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

    public void removeAllHeld(TransactionId tid) {
        for (LocksOnPage lop : this.locks.values()) {
            if (lop.holdsSharedLock(tid)) {
                lop.removeSharedLock(tid);
            }
            if (lop.holdsExclusiveLock(tid)) {
                lop.setExclusiveLock(null);
            }
        }
    }

    // Used to reset the lock manager.
    // Done by removing all locks stored in the manager.
    public void reset() {
        this.locks.clear();
    }

}
