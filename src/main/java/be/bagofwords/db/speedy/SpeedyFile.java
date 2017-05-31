package be.bagofwords.db.speedy;

import be.bagofwords.logging.Log;
import be.bagofwords.util.MappedLists;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
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
    public long firstKey;
    public long lastKey;
    private final ReadWriteLock lock;
    public SpeedyFile left;
    public SpeedyFile right;

    public SpeedyFile(long firstKey, long lastKey) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.size = 0;
        this.numOfKeys = 0;
        this.actualKeys = 0;
        this.lock = new ReentrantReadWriteLock();
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
        // Log.i("lockWrite " + this);
    }

    public void unlockWrite() {
        // Log.i("unlockWrite " + this);
        lock.writeLock().unlock();
    }

    public SpeedyFile getFile(long value, boolean lockRead) {
        if (left == null) {
            if (lockRead) {
                lockRead();
            } else {
                lockWrite();
            }
            if (left == null) {
                return this;
            } else {
                if (lockRead) {
                    unlockRead();
                } else {
                    unlockWrite();
                }
            }
        }
        if (right.firstKey > value) {
            return left.getFile(value, lockRead);
        } else {
            return right.getFile(value, lockRead);
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

    public <T> void mapValuesToNodes(MappedLists<SpeedyFile, Pair<Long, T>> result, List<Pair<Long, T>> values, int start, int end) {
        if (start >= end) {
            return;
        }
        if (left == null) {
            result.put(this, values.subList(start, end));
        } else {
            int splitInd = Collections.binarySearch(values, right.firstKey, (p, k) -> Long.compare(((Pair<Long, T>) p).getKey(), (Long) k));
            if (splitInd < 0) {
                splitInd = -(splitInd + 1);
            }
            left.mapValuesToNodes(result, values, start, splitInd);
            right.mapValuesToNodes(result, values, splitInd, end);
        }
    }
}

