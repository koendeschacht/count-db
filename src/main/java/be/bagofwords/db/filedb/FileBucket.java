package be.bagofwords.db.filedb;

import be.bagofwords.logging.Log;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class FileBucket implements Comparable<FileBucket> {

    private static final int NUMBER_OF_READ_PERMITS = 1000;

    private long firstKey; //inclusive
    private long lastKey; //inclusive
    private List<FileInfo> files;
    private final ReadWriteLock lock;
    private boolean shouldBeCleanedBeforeRead;

    public FileBucket(long firstKey, long lastKey) {
        this();
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.files = new ArrayList<>();
        this.shouldBeCleanedBeforeRead = false;
    }

    public List<FileInfo> getFiles() {
        return files;
    }

    public int getFileInd(long key) {
        if (files.size() < 10) {
            for (int i = 0; i < files.size(); i++) {
                if (files.get(i).getFirstKey() > key) {
                    if (i == 0) {
                        throw new RuntimeException("Incorrect bucket starting from " + getFirstKey() + " for key " + key);
                    } else {
                        return i - 1;
                    }
                }
            }
            return files.size() - 1;
        } else {
            int pos = Collections.binarySearch((List) files, key);
            if (pos < 0) {
                pos = -(pos + 2);
            }
            if (pos == -1) {
                Log.i("ARRAY INDEX OUT OF BOUNDS!!!!");
                Log.i("Was looking for key " + key);
                Log.i("In bucket starting with key " + getFirstKey());
                Log.i("In files ");
                for (FileInfo file : files) {
                    Log.i("   " + file.getFirstKey());
                }
            }
            return pos;
        }
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

    public void lockRead() {
        lock.readLock().lock();
    }

    public void unlockRead() {
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
    }

    public boolean tryLockWrite() {
        return lock.writeLock().tryLock();
    }

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

    public String toString() {
        return super.toString() + " " + firstKey;
    }

    @Override
    public int compareTo(FileBucket o) {
        return Long.compare(firstKey, o.getFirstKey());
    }

    public boolean shouldBeCleanedBeforeRead() {
        return shouldBeCleanedBeforeRead;
    }


    public void setShouldBeCleanedBeforeRead(boolean shouldBeCleanedBeforeRead) {
        this.shouldBeCleanedBeforeRead = shouldBeCleanedBeforeRead;
    }

    /**
     * Serialization:
     */

    public FileBucket() {
        this.lock = new ReentrantReadWriteLock();
    }

    public void setFiles(List<FileInfo> files) {
        this.files = files;
    }

    public boolean isShouldBeCleanedBeforeRead() {
        return shouldBeCleanedBeforeRead;
    }

    public void setFirstKey(long firstKey) {
        this.firstKey = firstKey;
    }

    public void setLastKey(long lastKey) {
        this.lastKey = lastKey;
    }
}
