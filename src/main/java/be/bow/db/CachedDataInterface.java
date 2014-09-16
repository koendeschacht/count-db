package be.bow.db;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.Cache;
import be.bow.cache.CacheableData;
import be.bow.cache.CachesManager;
import be.bow.iterator.CloseableIterator;
import be.bow.util.DataLock;
import be.bow.util.KeyValue;

import java.util.Iterator;
import java.util.List;

public class CachedDataInterface<T extends Object> extends LayeredDataInterface<T> implements CacheableData<T> {

    private final MemoryManager memoryManager;
    private final Cache<T> readCache;
    private final Cache<T> writeCache;
    private final DataLock writeLock;

    public CachedDataInterface(CachesManager cachesManager, MemoryManager memoryManager, DataInterface<T> baseInterface) {
        super(baseInterface);
        this.memoryManager = memoryManager;
        this.readCache = cachesManager.createNewCache(this, false, getName() + "_read");
        this.writeCache = cachesManager.createNewCache(this, true, getName() + "_write");
        this.writeLock = new DataLock();
    }

    @Override
    public T readInt(long key) {
        T value = readCache.get(key);
        if (value == null) {
            //Never read, read from direct
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
        memoryManager.waitForSufficientMemory();
        writeLock.lockWrite(key);
        nonSynchronizedWrite(key, value);
        writeLock.unlockWrite(key);
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
        baseInterface.write(entries);
    }

    @Override
    public synchronized void close() {
        flush();
        baseInterface.close();
    }

    public void flush() {
        flushWriteCache();
        baseInterface.flush();
    }

    private void flushWriteCache() {
        writeLock.lockWriteAll();
        writeCache.flush();
        writeLock.unlockWriteAll();
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
    public CloseableIterator<Long> keyIterator() {
        flushWriteCache();
        return baseInterface.keyIterator();
    }

    @Override
    public void removedValues(Cache cache, List<KeyValue<T>> valuesToRemove) {
        if (cache == writeCache) {
            int size = valuesToRemove.size();
            if (size > 0) {
                baseInterface.write(valuesToRemove.iterator());
            }
        }
    }

    @Override
    public void valuesChanged(long[] keys) {
        super.valuesChanged(keys);
        for (Long key : keys) {
            readCache.remove(key);
        }
    }

}


