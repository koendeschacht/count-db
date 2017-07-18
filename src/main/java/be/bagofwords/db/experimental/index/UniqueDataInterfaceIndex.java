package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.MetaDataStore;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.db.methods.LongObjectSerializer;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UniqueDataInterfaceIndex<T> extends BaseDataInterfaceIndex<T> {

    private final UniqueDataIndexer<T> indexer;
    private final BaseDataInterface<Long> indexedDataInterface;

    public UniqueDataInterfaceIndex(String name, DataInterfaceFactory dataInterfaceFactory, DataInterface<T> dataInterface, UniqueDataIndexer<T> indexer, MetaDataStore metaDataStore) {
        super(dataInterface, metaDataStore);
        this.indexer = indexer;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(name, Long.class, new UniqueKeyCombinator(), new LongObjectSerializer());
        this.lastSync = metaDataStore.getLong(indexedDataInterface, "last.sync", -Long.MAX_VALUE);
    }

    @Override
    protected String getIndexName() {
        return indexedDataInterface.getName();
    }

    @Override
    protected void rebuildIndex() {
        indexedDataInterface.dropAllData();
        lastSync = dataInterface.lastFlush();
        CloseableIterator<KeyValue<T>> it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<T> curr = it.next();
            T value = curr.getValue();
            long indexKey = indexer.convertToIndex(value);
            indexedDataInterface.write(indexKey, curr.getKey());
        }
        it.close();
        indexedDataInterface.flush();
        metaDataStore.write(indexedDataInterface, "last.sync", lastSync);
    }

    public T read(long indexKey) {
        ensureIndexUpToDate();
        Long key = indexedDataInterface.read(indexKey);
        if (key != null) {
            return dataInterface.read(key);
        } else {
            return null;
        }
    }

    public T read(T queryByObject) {
        ensureIndexUpToDate();
        long indexKey = indexer.convertToIndex(queryByObject);
        return read(indexKey);
    }

    public Stream<T> streamValues(KeyFilter keyFilter) {
        ensureIndexUpToDate();
        Stream<Long> keyStream = indexedDataInterface.streamValues(keyFilter);
        return streamValuesForKeys(keyStream);
    }

    public Stream<T> streamValues() {
        return streamValues(false);
    }

    public Stream<T> streamValues(boolean desc) {
        ensureIndexUpToDate();
        List<Long> uniqueIds = indexedDataInterface.streamValues()
                .collect(Collectors.toList());
        if (desc) {
            Collections.reverse(uniqueIds);
        }
        return streamValuesForKeys(uniqueIds.stream());
    }

    public void close() {
        this.indexedDataInterface.close();
    }
}
