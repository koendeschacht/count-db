package be.bagofwords.db;

import be.bagofwords.application.LateCloseableComponent;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.application.status.StatusViewable;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.bloomfilter.BloomFilterDataInterface;
import be.bagofwords.db.bloomfilter.LongBloomFilterWithCheckSum;
import be.bagofwords.db.cached.CachedDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.combinator.OverWriteCombinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class DataInterfaceFactory implements StatusViewable, LateCloseableComponent {

    private final CachesManager cachesManager;
    private MemoryManager memoryManager;
    private final List<DataInterface> allInterfaces;

    private DataInterface<LongBloomFilterWithCheckSum> cachedBloomFilters;
    private FlushDataInterfacesThread flushDataInterfacesThread;

    public DataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager) {
        this.cachesManager = cachesManager;
        this.memoryManager = memoryManager;
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
        flushDataInterfacesThread.terminate();
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

    @Override
    public void printHtmlStatus(StringBuilder sb) {
        sb.append("<h1>Data interfaces</h1>");
        List<DataInterface> interfaces = new ArrayList<>(getAllInterfaces());
        Collections.sort(interfaces, new Comparator<DataInterface>() {
            @Override
            public int compare(DataInterface o1, DataInterface o2) {
                long max1 = Math.max(o1.getTotalTimeRead(), o1.getTotalTimeWrite());
                long max2 = Math.max(o2.getTotalTimeRead(), o2.getTotalTimeWrite());
                return -Double.compare(max1, max2); //Highest first
            }
        });
        for (DataInterface dataInterface : interfaces) {
            printDataInterfaceUsage("&nbsp;&nbsp;&nbsp;", sb, dataInterface);
        }
    }

    protected void printDataInterfaceUsage(String indentation, StringBuilder sb, DataInterface dataInterface) {
        if (dataInterface.getNumberOfReads() + dataInterface.getNumberOfWrites() > 0 || dataInterface.getTotalTimeRead() > 0 || dataInterface.getTotalTimeWrite() > 0) {
            sb.append(indentation + dataInterface.getClass().getSimpleName() + " " + dataInterface.getName() + " reads=" + dataInterface.getNumberOfReads() + " readTime=" + dataInterface.getTotalTimeRead() + " writes=" + dataInterface.getNumberOfWrites() + " writeTime=" + dataInterface.getTotalTimeWrite());
            sb.append("<br>");
            DataInterface implementingDataInterface = dataInterface.getImplementingDataInterface();
            if (implementingDataInterface != null) {
                printDataInterfaceUsage(indentation + indentation, sb, implementingDataInterface);
            }
        }
    }

}
