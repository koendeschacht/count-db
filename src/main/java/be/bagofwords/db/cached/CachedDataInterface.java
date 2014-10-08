package be.bagofwords.db.cached;

import be.bagofwords.cache.Cache;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.FlushDataInterfacesThread;
import be.bagofwords.db.LayeredDataInterface;
import be.bagofwords.util.DataLock;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.Utils;

import java.util.Iterator;
import java.util.List;

public class CachedDataInterface<T extends Object> extends LayeredDataInterface<T> {

    private Cache<T> readCache;
    private Cache<T> writeCache;
    private final DataLock writeLock;
    private final String flushLock = new String("LOCK");
    private long timeOfLastFlush;

    public CachedDataInterface(CachesManager cachesManager, DataInterface<T> baseInterface) {
        super(baseInterface);
        this.readCache = cachesManager.createNewCache(false, getName() + "_read", baseInterface.getObjectClass());
        this.writeCache = cachesManager.createNewCache(true, getName() + "_write", baseInterface.getObjectClass());
        this.writeLock = new DataLock(10000, false);
    }

    @Override
    public T readInt(long key) {
        T value = readCache.get(key);
        if (value == null) {
            //never read, read from direct
            value = baseInterface.read(key);
            readCache.put(key, value);
        }
        return value;
    }

    @Override
    public boolean mightContain(long key) {
        T cachedValue = readCache.get(key);
        if (cachedValue != null) {
            return true;
        } else {
            return baseInterface.mightContain(key);
        }
    }

    @Override
    public void writeInt(long key, T value) {
        waitForSlowFlushes();
        writeLock.lockWrite(key);
        try {
            nonSynchronizedWrite(key, value);
        } finally {
            writeLock.unlockWrite(key);
        }
    }

    /**
     * When writing large amounts of data (especially when this is performed with multiple threads), the flushes of the
     * write cache might not be able to keep up. If flushes start taking really long (5 times normal time between flushes)
     * we momentarily pause writes to this cache.
     */

    private void waitForSlowFlushes() {
        long now = System.currentTimeMillis();
        while (now - timeOfLastFlush > FlushDataInterfacesThread.TIME_BETWEEN_FLUSHES * 5) {
            Utils.threadSleep(10);
        }
    }

    private void nonSynchronizedWrite(long key, T value) {
        T currentValue = writeCache.get(key);
        boolean combine = value != null; //Current action is not a delete
        combine = combine && currentValue != null; //Key is already in write cache
        if (combine) {
            T combinedValue = getCombinator().combine(currentValue, value);
            if (combinedValue != null) {
                writeCache.put(key, combinedValue);
            } else {
                writeCache.put(key, null);
            }
        } else {
            writeCache.put(key, value);
        }
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        flushWriteCache();
        baseInterface.write(entries);
    }

    @Override
    public synchronized void doClose() {
        try {
            flush();
        } finally {
            //even if the flush failed, we remove our data structures
            readCache.clear();
            readCache = null;
            writeCache.clear();
            writeCache = null;
            baseInterface.close();
        }
    }

    public void flush() {
        flushWriteCache();
        baseInterface.flush();
    }

    private void flushWriteCache() {
        //First lock on flushLock to make sure that flushes (including writing the values) happen in order
        synchronized (flushLock) {
            //Lock all write locks to make sure no values are written to the write buffer while we collect them all
            writeLock.lockWriteAll();
            List<KeyValue<T>> allValues = writeCache.removeAllValues();
            writeLock.unlockWriteAll();
            if (!allValues.isEmpty()) {
                baseInterface.write(allValues.iterator());
            }
            timeOfLastFlush = System.currentTimeMillis();
        }
    }

    @Override
    public void dropAllData() {
        writeCache.clear();
        readCache.clear();
        baseInterface.dropAllData();
    }

    public long apprSize() {
        return writeCache.size() + baseInterface.apprSize();
    }

    @Override
    public void valuesChanged(long[] keys) {
        super.valuesChanged(keys);
        for (Long key : keys) {
            readCache.remove(key);
        }
    }

}


