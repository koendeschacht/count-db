package be.bagofwords.db.newfiledb;

import be.bagofwords.db.speedy.SpeedyFile;
import be.bagofwords.logging.Log;
import be.bagofwords.util.Pair;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import static be.bagofwords.db.newfiledb.LockMethod.LOCK_READ;
import static be.bagofwords.db.newfiledb.LockMethod.LOCK_WRITE;

public class FileNode implements Comparable<FileNode> {

    public String fileName;
    public long firstKey; //inclusive
    public long lastKey; //inclusive
    private final ReadWriteLock lock;
    private boolean shouldBeCleanedBeforeRead;
    private FileNode parent;
    private FileNode left;
    private FileNode right;

    //This field is only filled in when the file is clean (i.e. not isDirty)
    public long[] fileLocationsKeys;
    public int[] fileLocationsValues;
    public int readSize;
    public int writeSize;
    public byte[] cachedFileContents;

    public FileNode(long firstKey, long lastKey) {
        this();
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.fileName = Long.toString(firstKey);
        this.shouldBeCleanedBeforeRead = false;
    }

    public FileNode getNode(long key, LockMethod lockMethod) {
        if (left == null) {
            acquireLock(lockMethod);
            //Check again that we don't have any children, it can have changed while acquiring lock
            if (left != null) {
                releaseLock(lockMethod);
            } else {
                return this;
            }
        }
        if (right.firstKey < key) {
            return left.getNode(key, lockMethod);
        } else {
            return right.getNode(key, lockMethod);
        }
    }

    private void releaseLock(LockMethod lockMethod) {
        if (lockMethod == LOCK_READ) {
            unlockRead();
        } else if (lockMethod == LOCK_WRITE) {
            unlockWrite();
        }
    }

    private void acquireLock(LockMethod lockMethod) {
        if (lockMethod == LOCK_READ) {
            lockRead();
        } else if (lockMethod == LOCK_WRITE) {
            lockWrite();
        }
    }

    public void lockRead() {
        Log.i("Lock read " + this);
        lock.readLock().lock();
    }

    public void unlockRead() {
        Log.i("Unlock read " + this);
        lock.readLock().unlock();
    }

    public void lockWrite() {
        Log.i("Lock write " + this);
        lock.writeLock().lock();
    }

    public boolean tryLockWrite() {
        return lock.writeLock().tryLock();
    }

    public void unlockWrite() {
        Log.i("Unlock write " + this);
        lock.writeLock().unlock();
    }

    public String toString() {
        return super.toString() + " " + firstKey;
    }

    @Override
    public int compareTo(FileNode o) {
        return Long.compare(firstKey, o.firstKey);
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

    public FileNode() {
        this.lock = new ReentrantReadWriteLock();
    }

    public boolean isShouldBeCleanedBeforeRead() {
        return shouldBeCleanedBeforeRead;
    }

    public void setChildNodes(FileNode left, FileNode right) {
        this.left = left;
        this.right = right;
        left.parent = this;
        right.parent = this;
    }

    public FileNode getLeft() {
        return left;
    }

    public FileNode getRight() {
        return right;
    }

    public void increaseWriteSize(int diff) {
        this.writeSize += diff;
    }

    public void doForAllLeafs(Consumer<FileNode> action, LockMethod lockMethod) {
        doForAllLeafs(action, lockMethod, true);
    }

    public void doForAllLeafs(Consumer<FileNode> action, LockMethod lockMethod, boolean unlock) {
        if (left == null) {
            acquireLock(lockMethod);
            try {
                if (left == null) {
                    action.accept(this);
                    if (unlock) {
                        releaseLock(lockMethod);
                    }
                    return;
                }
            } catch (Exception exp) {
                releaseLock(lockMethod);
                throw exp;
            }
        }
        left.doForAllLeafs(action, lockMethod, unlock);
        right.doForAllLeafs(action, lockMethod, unlock);
    }

    public long discardFileContents() {
        int bytesReleased = 0;
        if (cachedFileContents != null) {
            bytesReleased = cachedFileContents.length;
            cachedFileContents = null;
        }
        return bytesReleased;
    }

    @JsonIgnore
    public boolean isClean() {
        return readSize == writeSize;
    }

    public void fileWasRewritten(List<Pair<Long, Integer>> fileLocations, int newReadSize, int newWriteSize) {
        this.readSize = newReadSize;
        this.writeSize = newWriteSize;
        this.fileLocationsKeys = new long[fileLocations.size()];
        this.fileLocationsValues = new int[fileLocations.size()];
        for (int i = 0; i < fileLocations.size(); i++) {
            fileLocationsKeys[i] = fileLocations.get(i).getFirst();
            fileLocationsValues[i] = fileLocations.get(i).getSecond();
        }
    }

}
