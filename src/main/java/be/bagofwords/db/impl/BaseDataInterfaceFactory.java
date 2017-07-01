package be.bagofwords.db.impl;

import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceConfig;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.bloomfilter.BloomFilterDataInterface;
import be.bagofwords.db.bloomfilter.LongBloomFilterWithCheckSum;
import be.bagofwords.db.cached.CachedDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.experimental.index.DataIndexer;
import be.bagofwords.db.experimental.index.DataInterfaceIndex;
import be.bagofwords.db.memory.InMemoryDataInterface;
import be.bagofwords.jobs.AsyncJobService;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.LifeCycleBean;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

public abstract class BaseDataInterfaceFactory implements LifeCycleBean, DataInterfaceFactory {

    public static final String META_DATA_STORAGE = "system/metaData";

    private int tmpDataInterfaceCount = 0;

    private final CachesManager cachesManager;
    protected final MemoryManager memoryManager;
    protected final AsyncJobService asyncJobService;
    private final List<DataInterfaceReference> allInterfaces;
    private final ReferenceQueue<DataInterface> allInterfacesReferenceQueue;

    private BaseDataInterface<LongBloomFilterWithCheckSum> bloomFiltersInterface;
    private BaseDataInterface<String> metaDataInterface;

    private final MetaDataStore metaDataStore;

    public BaseDataInterfaceFactory(ApplicationContext context) {
        this.cachesManager = context.getBean(CachesManager.class);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.asyncJobService = context.getBean(AsyncJobService.class);
        this.allInterfaces = new ArrayList<>();
        this.allInterfacesReferenceQueue = new ReferenceQueue<>();
        this.metaDataStore = new MetaDataStore();
    }

    public <T> DataInterfaceConfig<T> dataInterface(String name, Class<T> objectClass) {
        return new DataInterfaceConfig<>(name, objectClass, this);
    }

    @Override
    public <T> DataInterfaceIndex<T> index(DataInterface<T> dataInterface, String nameOfIndex, DataIndexer<T> indexer) {
        return new DataInterfaceIndex<>(nameOfIndex, this, dataInterface, indexer, metaDataStore);
    }

    public <T> BaseDataInterface<T> createFromConfig(DataInterfaceConfig<T> config) {
        BaseDataInterface<T> dataInterface;
        String name = config.name;
        if (config.isTemporary) {
            name = createNameForTemporaryInterface(name);
        }
        if (config.inMemory) {
            dataInterface = new InMemoryDataInterface<>(name, config.objectClass, config.combinator);
        } else {
            dataInterface = createBaseDataInterface(name, config.objectClass, config.combinator, config.isTemporary);
        }
        setMetaDataStore(dataInterface);
        if (config.cache) {
            dataInterface = new CachedDataInterface<>(memoryManager, cachesManager, dataInterface, asyncJobService);
        }
        if (config.bloomFilter) {
            checkInitialisationCachedBloomFilters();
            dataInterface = new BloomFilterDataInterface<>(dataInterface, bloomFiltersInterface, asyncJobService);
        }
        registerInterface(dataInterface);
        return dataInterface;
    }

    private <T> void setMetaDataStore(BaseDataInterface<T> dataInterface) {
        if (dataInterface instanceof CoreDataInterface) {
            ((CoreDataInterface<Object>) dataInterface).setMetaDataStore(metaDataStore);
        }
    }

    private <T> void registerInterface(BaseDataInterface<T> dataInterface) {
        synchronized (allInterfaces) {
            allInterfaces.add(new DataInterfaceReference(dataInterface, allInterfacesReferenceQueue));
        }
    }

    protected abstract <T extends Object> BaseDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface);

    protected abstract Class<? extends DataInterface> getBaseDataInterfaceClass();

    public DataInterface<Long> createCountDataInterface(String name) {
        return createDataInterface(name, Long.class, new LongCombinator(), false, false);
    }

    public DataInterface<Long> createTmpCountDataInterface(String name) {
        return createDataInterface(createNameForTemporaryInterface(name), Long.class, new LongCombinator(), true, false);
    }

    public DataInterface<Long> createInMemoryCountDataInterface(String name) {
        return createDataInterface(name, Long.class, new LongCombinator(), false, true);
    }

    public <T extends Object> BaseDataInterface<T> createDataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(name, objectClass, combinator, false, false);
    }

    private <T extends Object> BaseDataInterface<T> createDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean temporary, boolean inMemory) {
        DataInterfaceConfig<T> config = dataInterface(name, objectClass);
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
        if (bloomFiltersInterface == null) {
            bloomFiltersInterface = createBaseDataInterface("system/bloomFilter", LongBloomFilterWithCheckSum.class, new OverWriteCombinator<>(), false);
            setMetaDataStore(bloomFiltersInterface);
            synchronized (allInterfaces) {
                allInterfaces.add(new DataInterfaceReference(bloomFiltersInterface, allInterfacesReferenceQueue));
            }
        }
    }

    public List<DataInterfaceReference> getAllInterfaces() {
        return allInterfaces;
    }

    @Override
    public void startBean() {
        BaseDataInterface<String> baseMetaDataStorage = createBaseDataInterface(META_DATA_STORAGE, String.class, new OverWriteCombinator<>(), false);
        metaDataInterface = new CachedDataInterface<>(memoryManager, cachesManager, baseMetaDataStorage, asyncJobService);
        metaDataStore.setStorage(metaDataInterface);
        registerInterface(metaDataInterface);
        if (baseMetaDataStorage instanceof CoreDataInterface) {
            ((CoreDataInterface) baseMetaDataStorage).setMetaDataStore(metaDataStore);
        }
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
                if (dataInterface != null && !isSystemInterface(dataInterface)) {
                    dataInterface.close();
                }
            }
            if (bloomFiltersInterface != null) {
                bloomFiltersInterface.close();
                bloomFiltersInterface = null;
            }
            allInterfaces.clear();
            metaDataStore.close();
        }

    }

    private boolean isSystemInterface(DataInterface dataInterface) {
        return dataInterface == bloomFiltersInterface || dataInterface == metaDataInterface;
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

    private String createNameForTemporaryInterface(String name) {
        return "tmp/" + name + "_" + System.currentTimeMillis() + "_" + tmpDataInterfaceCount++ + "/";
    }

}
