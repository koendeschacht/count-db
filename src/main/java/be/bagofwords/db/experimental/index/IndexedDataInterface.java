package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.LayeredDataInterface;
import be.bagofwords.db.data.LongList;
import be.bagofwords.db.data.LongListCombinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Attempt to add indexes to data interfaces. Not yet finished...
 */

public class IndexedDataInterface<T> extends LayeredDataInterface<T> {

    private final DataIndexer<T> indexer;
    private final BaseDataInterface<LongList> indexedDataInterface;

    public IndexedDataInterface(String name, DataInterfaceFactory dataInterfaceFactory, BaseDataInterface<T> baseInterface, DataIndexer<T> indexer) {
        super(baseInterface);
        this.indexer = indexer;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(name, LongList.class, new LongListCombinator());
    }

    @Override
    public void write(long key, T value) {
        baseInterface.write(key, value);
        for (long indexKey : indexer.convertToIndexes(value)) {
            indexedDataInterface.write(indexKey, new LongList(key));
        }
    }

    public void rebuildIndex() {
        indexedDataInterface.dropAllData();
        CloseableIterator<KeyValue<T>> it = baseInterface.iterator();
        while (it.hasNext()) {
            KeyValue<T> curr = it.next();
            T value = curr.getValue();
            for (long indexKey : indexer.convertToIndexes(value)) {
                indexedDataInterface.write(indexKey, new LongList(curr.getKey()));
            }
        }
        it.close();
        indexedDataInterface.flush();
    }

    public List<T> readIndexedValues(T queryByObject) {
        List<Long> indexKeys = indexer.convertToIndexes(queryByObject);
        Set<T> uniqueResults = new HashSet<>();
        for (Long indexKey : indexKeys) {
            uniqueResults.addAll(readIndexedValues(indexKey));
        }
        return new ArrayList<>(uniqueResults);
    }

    public List<T> readIndexedValues(long indexKey) {
        LongList keys = indexedDataInterface.read(indexKey);
        List<T> result = new ArrayList<>();
        if (keys != null) {
            for (long key : keys) {
                T value = baseInterface.read(key);
                if (value != null) {
                    result.add(value);
                }
            }
        }
        return result;
    }

    @Override
    public void dropAllData() {
        baseInterface.dropAllData();
        indexedDataInterface.dropAllData();
    }

    @Override
    public void flush() {
        baseInterface.flush();
        indexedDataInterface.flush();
    }

    @Override
    protected void doCloseImpl() {
        indexedDataInterface.close();
    }

}
