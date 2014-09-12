package be.bow.db;


import be.bow.db.data.LongList;
import be.bow.db.data.LongListCombinator;
import be.bow.iterator.CloseableIterator;
import be.bow.util.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class IndexedDataInterface<T> extends LayeredDataInterface<T> {

    private final DataIndexer<T> indexer;
    private final DataInterface<LongList> indexedDataInterface;

    public IndexedDataInterface(DataInterfaceFactory dataInterfaceFactory, DataInterface<T> baseInterface, DataIndexer<T> indexer) {
        super(baseInterface);
        this.indexer = indexer;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(DatabaseCachingType.CACHED_AND_BLOOM, baseInterface.getName() + "_idx", LongList.class, new LongListCombinator());
    }

    @Override
    protected void writeInt(long key, T value) {
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

    public List<T> readIndexedValues(long indexKey) {
        LongList keys = indexedDataInterface.read(indexKey);
        List<T> result = new ArrayList<>();
        if (keys != null) {
            for (long key : keys) {
                T value = baseInterface.read(key);
                if (value != null && indexer.convertToIndexes(value).contains(indexKey)) {
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
    public void close() {
        baseInterface.close();
        indexedDataInterface.close();
    }

}
