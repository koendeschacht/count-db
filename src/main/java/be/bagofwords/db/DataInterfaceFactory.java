package be.bagofwords.db;

import be.bagofwords.application.LateCloseableComponent;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.bloomfilter.BloomFilterDataInterface;
import be.bagofwords.db.bloomfilter.LongBloomFilterWithCheckSum;
import be.bagofwords.db.cached.CachedDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.combinator.OverWriteCombinator;

import java.util.ArrayList;
import java.util.List;

public abstract class DataInterfaceFactory implements LateCloseableComponent {

    private final CachesManager cachesManager;
    private final List<DataInterface> allInterfaces;

    private DataInterface<LongBloomFilterWithCheckSum> cachedBloomFilters;
    private FlushDataInterfacesThread flushDataInterfacesThread;

    public DataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager) {
        this.cachesManager = cachesManager;
        this.allInterfaces = new ArrayList<>();
        this.flushDataInterfacesThread = new FlushDataInterfacesThread(this, memoryManager);
        this.flushDataInterfacesThread.start();
    }

    protected abstract <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator);

    public DataInterface<Long> createCountDataInterface(String subset) {
        return createDataInterface(DatabaseCachingType.CACHED_AND_BLOOM, subset, Long.class, new LongCombinator());
    }

    public <T extends Object> DataInterface<T> createDataInterface(DatabaseCachingType type, String subset, Class<T> objectClass) {
        return createDataInterface(type, subset, objectClass, new OverWriteCombinator<T>());
    }

    public <T extends Object> DataInterface<T> createDataInterface(String subset, Class<T> objectClass) {
        return createDataInterface(DatabaseCachingType.CACHED_AND_BLOOM, subset, objectClass, new OverWriteCombinator<T>());
    }

    public <T extends Object> DataInterface<T> createDataInterface(final DatabaseCachingType type, final String subset, final Class<T> objectClass, final Combinator<T> combinator) {
        DataInterface<T> result = createBaseDataInterface(subset, objectClass, combinator);
        if (type.useCache()) {
            result = cached(result);
        }
        if (type.useBloomFilter()) {
            result = bloom(result);
        }
        synchronized (allInterfaces) {
            allInterfaces.add(result);
        }
        return result;
    }

    protected <T extends Object> DataInterface<T> cached(DataInterface<T> baseDataInterface) {
        return new CachedDataInterface<>(cachesManager, baseDataInterface);
    }

    protected <T extends Object> DataInterface<T> bloom(DataInterface<T> dataInterface) {
        checkInitialisationCachedBloomFilters();
        return new BloomFilterDataInterface<>(dataInterface, cachedBloomFilters);
    }

    private void checkInitialisationCachedBloomFilters() {
        if (cachedBloomFilters == null) {
            cachedBloomFilters = createBaseDataInterface("system/bloomFilter", LongBloomFilterWithCheckSum.class, new OverWriteCombinator<LongBloomFilterWithCheckSum>());
            synchronized (allInterfaces) {
                allInterfaces.add(cachedBloomFilters);
            }
        }
    }

    public List<DataInterface> getAllInterfaces() {
        return allInterfaces;
    }

    @Override
    public synchronized void terminate() {
        flushDataInterfacesThread.terminateAndWaitForFinish();
        closeAllInterfaces();
    }

    public void closeAllInterfaces() {
        synchronized (allInterfaces) {
            for (DataInterface dataI : allInterfaces) {
                if (dataI != cachedBloomFilters) {
                    dataI.flushIfNotClosed();
                    dataI.close();
                }
            }
            if (cachedBloomFilters != null) {
                cachedBloomFilters.flushIfNotClosed();
                cachedBloomFilters.close();
                cachedBloomFilters = null;
            }
            allInterfaces.clear();
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

}
