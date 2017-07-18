package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.data.LongList;
import be.bagofwords.db.data.LongListCombinator;
import be.bagofwords.db.data.LongListSerializer;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.MetaDataStore;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.db.methods.SetKeyFilter;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiDataInterfaceIndex<T> extends BaseDataInterfaceIndex<T> {

    private final MultiDataIndexer<T> indexer;
    private final BaseDataInterface<LongList> indexedDataInterface;

    public MultiDataInterfaceIndex(String name, DataInterfaceFactory dataInterfaceFactory, DataInterface<T> dataInterface, MultiDataIndexer<T> indexer, MetaDataStore metaDataStore) {
        super(dataInterface, metaDataStore);
        this.indexer = indexer;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(name, LongList.class, new LongListCombinator(), new LongListSerializer());
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
            for (long indexKey : indexer.convertToIndexes(value)) {
                indexedDataInterface.write(indexKey, new LongList(curr.getKey()));
            }
        }
        it.close();
        indexedDataInterface.flush();
        metaDataStore.write(indexedDataInterface, "last.sync", lastSync);
    }

    public List<T> read(long indexKey) {
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

    public List<T> read(T queryByObject) {
        return streamValues(queryByObject).collect(Collectors.toList());
    }

    public Stream<T> streamValues(T queryByObject) {
        ensureIndexUpToDate();
        List<Long> indexKeys = indexer.convertToIndexes(queryByObject);
        Stream<Long> keyStream = indexedDataInterface.streamValues(new SetKeyFilter(indexKeys))
                .flatMap(Collection::stream)
                .distinct();
        return streamValuesForKeys(keyStream);
    }

    public Stream<T> streamValues(KeyFilter keyFilter) {
        ensureIndexUpToDate();
        Stream<Long> keyStream = indexedDataInterface.streamValues(keyFilter)
                .flatMap(Collection::stream)
                .distinct();
        return streamValuesForKeys(keyStream);
    }

    public Stream<T> streamValues() {
        return streamValues(false);
    }

    public Stream<T> streamValues(boolean desc) {
        ensureIndexUpToDate();
        List<Long> uniqueIds = indexedDataInterface.streamValues().flatMap(Collection::stream)
                .distinct()
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
