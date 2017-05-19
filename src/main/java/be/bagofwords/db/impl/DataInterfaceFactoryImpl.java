package be.bagofwords.db.impl;

import be.bagofwords.application.TaskSchedulerService;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceConfig;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.bloomfilter.BloomFilterDataInterface;
import be.bagofwords.db.bloomfilter.LongBloomFilterWithCheckSum;
import be.bagofwords.db.cached.CachedDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.memory.InMemoryDataInterface;
import be.bagofwords.logging.Log;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.LifeCycleBean;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

public abstract class DataInterfaceFactoryImpl implements LifeCycleBean, DataInterfaceFactory {

    private int tmpDataInterfaceCount = 0;

    private final CachesManager cachesManager;
    private final MemoryManager memoryManager;
    protected final TaskSchedulerService taskScheduler;
    private final List<DataInterfaceReference> allInterfaces;
    private final ReferenceQueue<DataInterface> allInterfacesReferenceQueue;
    private final MetaDataStore metaDataStore;
    private boolean createdMetaDataStore;
    private BaseDataInterface<LongBloomFilterWithCheckSum> cachedBloomFilters;

    public DataInterfaceFactoryImpl(ApplicationContext context) {
        this.cachesManager = context.getBean(CachesManager.class);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.taskScheduler = context.getBean(TaskSchedulerService.class);
        this.allInterfaces = new ArrayList<>();
        this.allInterfacesReferenceQueue = new ReferenceQueue<>();
        this.metaDataStore = new MetaDataStore();
    }

    protected synchronized MetaDataStore getMetaDataStore() {
        if (!createdMetaDataStore) {
            createdMetaDataStore = true;
            Log.i("Creating meta_data store");
            BaseDataInterface<String> metaStoreInterface = createBaseDataInterface("meta_data", String.class, new OverWriteCombinator<>(), false);
            metaStoreInterface = new CachedDataInterface<>(memoryManager, cachesManager, metaStoreInterface, taskScheduler);
            registerInterface(metaStoreInterface);
            this.metaDataStore.setStorage(metaStoreInterface);
        }
        return this.metaDataStore;
    }

    public <T> DataInterfaceConfig<T> dataInterface(String nameOfSubset, Class<T> objectClass) {
        return new DataInterfaceConfig<>(nameOfSubset, objectClass, this);
    }

    public <T> BaseDataInterface<T> createFromConfig(DataInterfaceConfig<T> config) {
        BaseDataInterface<T> dataInterface;
        String subsetName = config.subsetName;
        if (config.isTemporary) {
            subsetName = createNameForTemporaryInterface(subsetName);
        }
        if (config.inMemory) {
            dataInterface = new InMemoryDataInterface<>(subsetName, config.objectClass, config.combinator, metaDataStore);
        } else {
            dataInterface = createBaseDataInterface(subsetName, config.objectClass, config.combinator, config.isTemporary);
        }
        if (config.cache) {
            dataInterface = new CachedDataInterface<>(memoryManager, cachesManager, dataInterface, taskScheduler);
        }
        if (config.bloomFilter) {
            checkInitialisationCachedBloomFilters();
            dataInterface = new BloomFilterDataInterface<>(dataInterface, cachedBloomFilters, taskScheduler);
        }
        registerInterface(dataInterface);
        return dataInterface;
    }

    private <T> void registerInterface(BaseDataInterface<T> dataInterface) {
        synchronized (allInterfaces) {
            allInterfaces.add(new DataInterfaceReference(dataInterface, allInterfacesReferenceQueue));
        }
    }

    protected abstract <T extends Object> BaseDataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface);

    public DataInterface<Long> createCountDataInterface(String subset) {
        return createDataInterface(subset, Long.class, new LongCombinator(), false, false);
    }

    public DataInterface<Long> createTmpCountDataInterface(String subset) {
        return createDataInterface(createNameForTemporaryInterface(subset), Long.class, new LongCombinator(), true, false);
    }

    public DataInterface<Long> createInMemoryCountDataInterface(String name) {
        return createDataInterface(name, Long.class, new LongCombinator(), false, true);
    }

    public <T extends Object> BaseDataInterface<T> createDataInterface(String subset, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(subset, objectClass, combinator, false, false);
    }

    private <T extends Object> BaseDataInterface<T> createDataInterface(String subset, Class<T> objectClass, Combinator<T> combinator, boolean temporary, boolean inMemory) {
        DataInterfaceConfig<T> config = dataInterface(subset, objectClass);
        config.combinator(combinator);
        if (temporary) {
            config.temporary();
        }
        if (inMemory) {
            config.inMemory();
        }
        return config.create();
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

}
