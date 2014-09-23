package be.bagofwords.db.filedb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

public class FileBucket implements Comparable<FileBucket> {

    private static final int NUMBER_OF_READ_PERMITS = 1000;

    private final long firstKey; //inclusive
    private final long lastKey; //inclusive
    private final List<FileInfo> files;
    private final Semaphore lock;

    public FileBucket(long firstKey, long lastKey) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.files = new ArrayList<>();
        this.lock = new Semaphore(NUMBER_OF_READ_PERMITS);
    }

    public List<FileInfo> getFiles() {
        return files;
    }

    public int getFileInd(long key) {
        int pos = Collections.binarySearch((List) files, key);
        if (pos < 0) {
            pos = -(pos + 2);
        }
        return pos;
    }

    public long getFirstKey() {
        return firstKey;
    }

    public FileInfo getFile(long key) {
        return files.get(getFileInd(key));
    }

    public long getLastKey() {
        return lastKey;
    }

    public void unlockWrite() {
        lock.release(NUMBER_OF_READ_PERMITS);
        if (lock.availablePermits() > NUMBER_OF_READ_PERMITS) {
            throw new RuntimeException("Illegal state of lock: too many unlocks");
        }
    }

    public void lockWrite() {
        lock.acquireUninterruptibly(NUMBER_OF_READ_PERMITS);
    }

    public void unlockRead() {
        lock.release(1);
        if (lock.availablePermits() > NUMBER_OF_READ_PERMITS) {
            throw new RuntimeException("Illegal state of lock: too many unlocks");
        }
    }

    public void lockRead() {
        lock.acquireUninterruptibly(1);
    }

    public boolean tryLockRead() {
        return lock.tryAcquire(1);
    }

    public String toString() {
        return super.toString() + " " + firstKey + ", permits=" + lock.availablePermits();
    }

    @Override
    public int compareTo(FileBucket o) {
        return Long.compare(firstKey, o.getFirstKey());
    }
}
