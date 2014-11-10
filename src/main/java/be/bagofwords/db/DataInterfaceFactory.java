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

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

public abstract class DataInterfaceFactory implements LateCloseableComponent {

    private int tmpDataInterfaceCount = 0;

    private CachesManager cachesManager;
    private MemoryManager memoryManager;
    private List<DataInterfaceReference> allInterfaces;
    private ReferenceQueue<DataInterface> allInterfacesReferenceQueue;

    private DataInterface<LongBloomFilterWithCheckSum> cachedBloomFilters;
    private DataInterfaceFactoryOccasionalActionsThread occasionalActionsThread;

    public DataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager) {
        this.cachesManager = cachesManager;
        this.memoryManager = memoryManager;
        this.allInterfaces = new ArrayList<>();
        this.allInterfacesReferenceQueue = new ReferenceQueue<>();
        this.occasionalActionsThread = new DataInterfaceFactoryOccasionalActionsThread(this, memoryManager);
        this.occasionalActionsThread.start();
    }

    public abstract <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface);

    public DataInterface<Long> createCountDataInterface(String subset) {
        return createDataInterface(DatabaseCachingType.CACHED_AND_BLOOM, subset, Long.class, new LongCombinator(), false);
    }

    public DataInterface<Long> createTmpCountDataInterface(String subset) {
        return createDataInterface(DatabaseCachingType.CACHED_AND_BLOOM, createNameForTemporaryInterface(subset), Long.class, new LongCombinator(), true);
    }

    public <T extends Object> DataInterface<T> createDataInterface(DatabaseCachingType type, String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(type, subset, objectClass, combinator, false);
    }

    public <T extends Object> DataInterface<T> createTmpDataInterface(DatabaseCachingType type, String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(type, createNameForTemporaryInterface(subset), objectClass, combinator, true);
    }

    public <T extends Object> DataInterface<T> createDataInterface(DatabaseCachingType type, String subset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        DataInterface<T> result = createBaseDataInterface(subset, objectClass, combinator, isTemporaryDataInterface);
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
        return new CachedDataInterface<>(memoryManager, cachesManager, baseDataInterface);
    }

    protected <T extends Object> DataInterface<T> bloom(DataInterface<T> dataInterface) {
        checkInitialisationCachedBloomFilters();
        return new BloomFilterDataInterface<>(dataInterface, cachedBloomFilters);
    }

    private void checkInitialisationCachedBloomFilters() {
        if (cachedBloomFilters == null) {
            cachedBloomFilters = createBaseDataInterface("system/bloomFilter", LongBloomFilterWithCheckSum.class, new OverWriteCombinator<LongBloomFilterWithCheckSum>(), false);
            synchronized (allInterfaces) {
                allInterfaces.add(new DataInterfaceReference(cachedBloomFilters, allInterfacesReferenceQueue));
            }
        }
    }

    public List<DataInterfaceReference> getAllInterfaces() {
        return allInterfaces;
    }

    @Override
    public synchronized void terminate() {
        occasionalActionsThread.terminateAndWaitForFinish();
        closeAllInterfaces();
    }

    public void closeAllInterfaces() {
        synchronized (allInterfaces) {
            for (WeakReference<DataInterface> referenceToDI : allInterfaces) {
                final DataInterface dataInterface = referenceToDI.get();
                if (dataInterface != null && dataInterface != cachedBloomFilters) {
                    dataInterface.doActionIfNotClosed(new DataInterface.ActionIfNotClosed() {
                        @Override
                        public void doAction() {
                            dataInterface.flush();
                            dataInterface.close();
                        }
                    });
                }
            }
            if (cachedBloomFilters != null) {
                cachedBloomFilters.doActionIfNotClosed(new DataInterface.ActionIfNotClosed() {
                    @Override
                    public void doAction() {
                        cachedBloomFilters.flush();
                        cachedBloomFilters.close();
                    }
                });
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
