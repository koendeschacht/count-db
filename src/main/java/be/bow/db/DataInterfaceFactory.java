package be.bow.db;

import be.bow.application.LateCloseableComponent;
import be.bow.application.memory.MemoryManager;
import be.bow.application.status.StatusViewable;
import be.bow.cache.CachesManager;
import be.bow.db.bloomfilter.BloomFilterDataInterface;
import be.bow.db.bloomfilter.LongBloomFilterWithCheckSum;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class DataInterfaceFactory implements StatusViewable, LateCloseableComponent {

    private final CachesManager cachesManager;
    private MemoryManager memoryManager;
    private final List<DataInterface> allInterfaces;

    private DataInterface<LongBloomFilterWithCheckSum> cachedBloomFilters;

    public DataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager) {
        this.cachesManager = cachesManager;
        this.memoryManager = memoryManager;
        this.allInterfaces = new ArrayList<>();
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
        if (type == DatabaseCachingType.CACHED || type == DatabaseCachingType.CACHED_AND_BLOOM) {
            result = cached(result);
        }
        if (type == DatabaseCachingType.CACHED_AND_BLOOM) {
            result = bloom(result);
        }
        synchronized (allInterfaces) {
            allInterfaces.add(result);
        }
        return result;
    }

    protected <T extends Object> DataInterface<T> cached(DataInterface<T> baseDataInterface) {
        return new CachedDataInterface<>(cachesManager, memoryManager, baseDataInterface);
    }

    protected <T extends Object> DataInterface<T> bloom(DataInterface<T> dataInterface) {
        if (cachedBloomFilters == null) {
            cachedBloomFilters = cached(createBaseDataInterface("system/bloomFilter", LongBloomFilterWithCheckSum.class, new OverWriteCombinator<LongBloomFilterWithCheckSum>()));
        }
        return new BloomFilterDataInterface<>(dataInterface, cachedBloomFilters);
    }

    public List<DataInterface> getAllInterfaces() {
        return allInterfaces;
    }

    @Override
    public synchronized void close() {
        closeAllInterfaces();
        if (cachedBloomFilters != null) {
            cachedBloomFilters.close();
        }
    }

    public void closeAllInterfaces() {
        synchronized (allInterfaces) {
            for (DataInterface dataI : allInterfaces) {
                dataI.flush();
                dataI.close();
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
