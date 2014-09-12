package be.bow.db.filedb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

public class FileBucket implements Comparable<FileBucket> {

    private final long firstKey; //inclusive
    private final long lastKey; //inclusive
    private final List<FileInfo> files;
    private final Semaphore lock;

    public FileBucket(long firstKey, long lastKey) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.files = new ArrayList<>();
        this.lock = new Semaphore(1);
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

    public void unlock() {
        lock.release();
        if (lock.availablePermits() > 1) {
            throw new RuntimeException("Illegal state of lock: too many unlocks");
        }
    }

    public void lock() {
        lock.acquireUninterruptibly();
    }

    public boolean tryLock() {
        return lock.tryAcquire();
    }

    public String toString() {
        return "FileBucket " + firstKey + ", permits=" + lock.availablePermits();
    }

    @Override
    public int compareTo(FileBucket o) {
        return Long.compare(firstKey, o.getFirstKey());
    }
}
