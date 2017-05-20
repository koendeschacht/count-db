package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.data.LongList;
import be.bagofwords.db.data.LongListCombinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.MetaDataStore;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataInterfaceIndex<T> {

    private final DataInterface<T> dataInterface;
    private final DataIndexer<T> indexer;
    private final MetaDataStore metaDataStore;
    private final BaseDataInterface<LongList> indexedDataInterface;
    private long lastSync = 0;
    private final Object buildIndexLock = new Object();

    public DataInterfaceIndex(String name, DataInterfaceFactory dataInterfaceFactory, DataInterface<T> dataInterface, DataIndexer<T> indexer, MetaDataStore metaDataStore) {
        this.dataInterface = dataInterface;
        this.indexer = indexer;
        this.metaDataStore = metaDataStore;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(name, LongList.class, new LongListCombinator());
        this.lastSync = metaDataStore.getLong(indexedDataInterface, "last.sync", -Long.MAX_VALUE);
    }

    private void rebuildIndex() {
        indexedDataInterface.dropAllData();
        lastSync = dataInterface.lastFlush();
        CloseableIterator<KeyValue<T>> it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<T> curr = it.next();
            T value = curr.getValue();
            for (long indexKey : indexer.convertToIndexes(value)) {
                indexedDataInterface.write(indexKey, new LongList(curr.getKey()));
            }
        }
        it.close();
        indexedDataInterface.flush();
        metaDataStore.write(indexedDataInterface, "last.sync", lastSync);
    }

    public List<T> readIndexedValues(T queryByObject) {
        ensureIndexUpToDate();
        List<Long> indexKeys = indexer.convertToIndexes(queryByObject);
        Set<T> uniqueResults = new HashSet<>();
        for (Long indexKey : indexKeys) {
            uniqueResults.addAll(readIndexedValues(indexKey));
        }
        return new ArrayList<>(uniqueResults);
    }

    public List<T> readIndexedValues(long indexKey) {
        ensureIndexUpToDate();
        LongList keys = indexedDataInterface.read(indexKey);
        List<T> result = new ArrayList<>();
        if (keys != null) {
            for (long key : keys) {
                T value = dataInterface.read(key);
                if (value != null) {
                    result.add(value);
                }
            }
        }
        return result;
    }

    private void ensureIndexUpToDate() {
        synchronized (buildIndexLock) {
            if (dataInterface.lastFlush() != lastSync) {
                Log.i("Index out of date, rebuilding index " + indexedDataInterface.getName());
                long start = System.currentTimeMillis();
                rebuildIndex();
                Log.i("Rebuilding index " + indexedDataInterface.getName() + " took " + (System.currentTimeMillis() - start) + " ms");
            }
        }
    }

    public void close() {
        this.indexedDataInterface.close();
    }
}
