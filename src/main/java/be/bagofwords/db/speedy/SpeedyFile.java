package be.bagofwords.db.speedy;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by koen on 29/05/17.
 */
public class SpeedyFile implements Comparable<SpeedyFile> {

    public long minKey;
    public long firstKey; //Since last rewrite
    public long lastKey; //Since last rewrite
    public int numOfKeys; //Since last rewrite
    public int actualKeys;
    public long size;
    private final ReadWriteLock lock;

    public SpeedyFile(long minKey, long firstKey, long lastKey, long size, int numOfKeys) {
        this.minKey = minKey;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.size = size;
        this.numOfKeys = numOfKeys;
        this.actualKeys = numOfKeys;
        this.lock = new ReentrantReadWriteLock();
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

    public void unlockWrite() {
        lock.writeLock().unlock();
    }

    @Override
    public int compareTo(SpeedyFile o) {
        return Long.compare(firstKey, o.firstKey);
    }

    @Override
    public String toString() {
        return "SpeedyFile{" +
                "minKey=" + minKey +
                ", firstKey=" + firstKey +
                ", lastKey=" + lastKey +
                ", numOfKeys=" + numOfKeys +
                ", size=" + size +
                '}';
    }
}
