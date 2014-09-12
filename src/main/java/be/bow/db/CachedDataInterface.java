package be.bow.db;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.CacheImportance;
import be.bow.cache.CacheableData;
import be.bow.cache.CachesManager;
import be.bow.iterator.CloseableIterator;
import be.bow.util.KeyValue;

import java.util.Iterator;
import java.util.List;

public class CachedDataInterface<T extends Object> extends LayeredDataInterface<T> implements CacheableData<T> {

    private final MemoryManager memoryManager;
    private final CachesManager cachesManager;
    private int readCacheInd;
    private int writeCacheInd;

    public CachedDataInterface(CachesManager cachesManager, MemoryManager memoryManager, DataInterface<T> baseInterface) {
        super(baseInterface);
        this.cachesManager = cachesManager;
        this.memoryManager = memoryManager;
        this.readCacheInd = cachesManager.createNewCache(this, false);
        this.writeCacheInd = cachesManager.createNewCache(this, true);
    }

    @Override
    public T readInt(long key) {
        T value = cachesManager.get(readCacheInd, key);
        if (value == null) {
            //Never read, read from direct
            value = baseInterface.read(key);
            cachesManager.put(readCacheInd, key, value);
        }
        return value;
    }

    @Override
    public boolean mightContain(long key) {
        T cachedValue = cachesManager.get(readCacheInd, key);
        if (cachedValue != null) {
            return true;
        } else {
            return baseInterface.mightContain(key);
        }
    }

    @Override
    public void writeInt(long key, T value) {
        memoryManager.waitForSufficientMemory();
        cachesManager.lockWrite(writeCacheInd, key);
        nonSynchronizedWrite(key, value);
        cachesManager.unlockWrite(writeCacheInd, key);
    }

    private void nonSynchronizedWrite(long key, T value) {
        T currentValue = cachesManager.get(writeCacheInd, key);
        boolean combine = value != null; //Current action is not a delete
        combine = combine && currentValue != null; //Key is already in write cache
        if (combine) {
            T combinedValue = getCombinator().combine(currentValue, value);
            if (combinedValue != null) {
                cachesManager.put(writeCacheInd, key, combinedValue);
            } else {
                cachesManager.put(writeCacheInd, key, null);
            }
        } else {
            cachesManager.put(writeCacheInd, key, value);
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
        cachesManager.flush(writeCacheInd);
        baseInterface.flush();
    }

    @Override
    public void dropAllData() {
        cachesManager.clear(writeCacheInd);
        cachesManager.clear(readCacheInd);
        baseInterface.dropAllData();
    }

    public long apprSize() {
        return cachesManager.getCache(writeCacheInd).size() + baseInterface.apprSize();
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        cachesManager.flush(writeCacheInd);
        return baseInterface.keyIterator();
    }

    @Override
    public void removedValues(int ind, List<KeyValue<T>> valuesToRemove) {
        if (ind == writeCacheInd) {
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
            cachesManager.remove(readCacheInd, key);
        }
    }

    @Override
    public CacheImportance getImportance() {
        return CacheImportance.DEFAULT;
    }


}


