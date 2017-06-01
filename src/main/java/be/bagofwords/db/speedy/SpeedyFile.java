package be.bagofwords.db.speedy;

import be.bagofwords.logging.Log;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

/**
 * Created by koen on 29/05/17.
 */
public class SpeedyFile {

    public int numOfKeys; // Since last rewrite
    public int actualKeys;
    public long size;
    public final long firstKey;
    public final long lastKey;
    public final File file;
    private final ReadWriteLock lock;
    private SpeedyFile left;
    private SpeedyFile right;
    public FileChannel writeFile;

    public SpeedyFile(long firstKey, long lastKey, FileChannel writeFile, File file) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.file = file;
        this.size = 0;
        this.numOfKeys = 0;
        this.actualKeys = 0;
        this.lock = new ReentrantReadWriteLock();
        this.writeFile = writeFile;
    }

    public void lockRead() {
        lock.readLock().lock();
        // Log.i("lockRead " + this);
    }

    public void unlockRead() {
        // Log.i("unlockRead " + this);
        lock.readLock().unlock();
    }

    public void lockWrite() {
        lock.writeLock().lock();
        // Log.i("lockWrite " + this + " from " + Thread.currentThread());
    }

    public void unlockWrite() {
        // Log.i("unlockWrite " + this + " from " + Thread.currentThread());
        lock.writeLock().unlock();
    }

    public SpeedyFile getFile(long value, LockMethod lockMethod) {
        if (left == null) {
            if (lockMethod == LockMethod.LOCK_READ) {
                lockRead();
            } else if (lockMethod == LockMethod.LOCK_WRITE) {
                lockWrite();
            }
            if (left == null) {
                return this;
            } else {
                if (lockMethod == LockMethod.LOCK_READ) {
                    unlockRead();
                } else if (lockMethod == LockMethod.LOCK_WRITE) {
                    unlockWrite();
                }
            }
        }
        if (right.firstKey > value) {
            return left.getFile(value, lockMethod);
        } else {
            return right.getFile(value, lockMethod);
        }
    }

    public void doForAllLeafs(Consumer<SpeedyFile> action) {
        if (left == null) {
            lockWrite();
            action.accept(this);
            unlockWrite();
        } else {
            left.doForAllLeafs(action);
            right.doForAllLeafs(action);
        }
    }

    public long getTotalNumberOfKeys() {
        if (left == null) {
            return actualKeys;
        } else {
            return left.getTotalNumberOfKeys() + right.getTotalNumberOfKeys();
        }
    }

    @Override
    public String toString() {
        return "SpeedyFile{" +
                "firstKey=" + firstKey +
                ", lastKey=" + lastKey +
                '}';
    }

    public boolean handlesKey(Long key) {
        return firstKey <= key && lastKey > key;
    }

    public int getNumberOfLeafs() {
        if (left == null) {
            return 1;
        } else {
            return left.getNumberOfLeafs() + right.getNumberOfLeafs();
        }
    }

    public void setChildNodes(SpeedyFile left, SpeedyFile right) {
        this.left = left;
        this.right = right;
        this.numOfKeys = 0;
        this.actualKeys = 0;
        IOUtils.closeQuietly(this.writeFile);
        this.writeFile = null;
    }

    public SpeedyFile getLeft() {
        return left;
    }

    public SpeedyFile getRight() {
        return right;
    }
}

