package be.bagofwords.db;

import be.bagofwords.application.BowTaskScheduler;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.bloomfilter.BloomFilterDataInterface;
import be.bagofwords.db.bloomfilter.LongBloomFilterWithCheckSum;
import be.bagofwords.db.cached.CachedDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.memory.InMemoryDataInterface;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.LifeCycleBean;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

public abstract class DataInterfaceFactory implements LifeCycleBean {

    private int tmpDataInterfaceCount = 0;

    private CachesManager cachesManager;
    private MemoryManager memoryManager;
    protected BowTaskScheduler taskScheduler;
    private List<DataInterfaceReference> allInterfaces;
    private ReferenceQueue<DataInterface> allInterfacesReferenceQueue;

    private DataInterface<LongBloomFilterWithCheckSum> cachedBloomFilters;

    public DataInterfaceFactory(ApplicationContext context) {
        this.cachesManager = context.getBean(CachesManager.class);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.taskScheduler = context.getBean(BowTaskScheduler.class);
        this.allInterfaces = new ArrayList<>();
        this.allInterfacesReferenceQueue = new ReferenceQueue<>();
    }

    public abstract <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface);

    public DataInterface<Long> createCountDataInterface(String subset) {
        return createDataInterface(DatabaseCachingType.CACHED, subset, Long.class, new LongCombinator(), false);
    }

    public DataInterface<Long> createTmpCountDataInterface(String subset) {
        return createDataInterface(DatabaseCachingType.CACHED, createNameForTemporaryInterface(subset), Long.class, new LongCombinator(), true);
    }

    public <T extends Object> DataInterface<T> createDataInterface(String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(DatabaseCachingType.CACHED, subset, objectClass, combinator, false);
    }

    public <T extends Object> DataInterface<T> createDataInterface(DatabaseCachingType cachingType, String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(cachingType, subset, objectClass, combinator, false);
    }

    public DataInterface<Long> createInMemoryCountDataInterface(String name) {
        return createInMemoryDataInterface(DatabaseCachingType.CACHED, name, Long.class, new LongCombinator());
    }

    public <T extends Object> DataInterface<T> createInMemoryDataInterface(DatabaseCachingType cachingType, String name, Class<T> objectClass, Combinator<T> combinator) {
        DataInterface<T> result = new InMemoryDataInterface<>(name, objectClass, combinator);
        result = decorateAndAdd(cachingType, result);
        return result;
    }

    public <T extends Object> DataInterface<T> createTmpDataInterface(String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(DatabaseCachingType.CACHED, createNameForTemporaryInterface(subset), objectClass, combinator, true);
    }

    public <T extends Object> DataInterface<T> createTmpDataInterface(DatabaseCachingType cachingType, String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(cachingType, createNameForTemporaryInterface(subset), objectClass, combinator, true);
    }

    public <T extends Object> DataInterface<T> createDataInterface(DatabaseCachingType cachingType, String subset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        DataInterface<T> result = createBaseDataInterface(subset, objectClass, combinator, isTemporaryDataInterface);
        result = decorateAndAdd(cachingType, result);
        return result;
    }

    private <T extends Object> DataInterface<T> decorateAndAdd(DatabaseCachingType type, DataInterface<T> result) {
        if (type.useCache()) {
            result = cached(result);
        }
        if (type.useBloomFilter()) {
            result = bloom(result);
        }
        synchronized (allInterfaces) {
            allInterfaces.add(new DataInterfaceReference(result, allInterfacesReferenceQueue));
        }
        return result;
    }

    protected <T extends Object> DataInterface<T> cached(DataInterface<T> baseDataInterface) {
        return new CachedDataInterface<>(memoryManager, cachesManager, baseDataInterface, taskScheduler);
    }

    protected <T extends Object> DataInterface<T> bloom(DataInterface<T> dataInterface) {
        checkInitialisationCachedBloomFilters();
        return new BloomFilterDataInterface<>(dataInterface, cachedBloomFilters, taskScheduler);
    }

    private void checkInitialisationCachedBloomFilters() {
        if (cachedBloomFilters == null) {
            cachedBloomFilters = createBaseDataInterface("system/bloomFilter", LongBloomFilterWithCheckSum.class, new OverWriteCombinator<>(), false);
            synchronized (allInterfaces) {
                allInterfaces.add(new DataInterfaceReference(cachedBloomFilters, allInterfacesReferenceQueue));
            }
        }
    }

    public List<DataInterfaceReference> getAllInterfaces() {
        return allInterfaces;
    }

    @Override
    public void startBean() {

    }

    @Override
    public synchronized void stopBean() {
        terminate();
    }

    public synchronized void terminate() {
        closeAllInterfaces();
    }

    public void closeAllInterfaces() {
        synchronized (allInterfaces) {
            for (WeakReference<DataInterface> referenceToDI : allInterfaces) {
                final DataInterface dataInterface = referenceToDI.get();
                if (dataInterface != null && dataInterface != cachedBloomFilters) {
                    dataInterface.close();
                }
            }
            if (cachedBloomFilters != null) {
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

    public void cleanupClosedInterfaces() {
        DataInterfaceReference reference = (DataInterfaceReference) allInterfacesReferenceQueue.poll();
        while (reference != null) {
            synchronized (allInterfaces) {
                allInterfaces.remove(reference);
            }
            reference = (DataInterfaceReference) allInterfacesReferenceQueue.poll();
        }
    }

    private String createNameForTemporaryInterface(String subset) {
        return "tmp/" + subset + "_" + System.currentTimeMillis() + "_" + tmpDataInterfaceCount++ + "/";
    }

    public static class DataInterfaceReference extends WeakReference<DataInterface> {

        private String subsetName;

        public DataInterfaceReference(DataInterface referent, ReferenceQueue<DataInterface> referenceQueue) {
            super(referent, referenceQueue);
            this.subsetName = referent.getName();
        }

        public String getSubsetName() {
            return subsetName;
        }
    }

}
