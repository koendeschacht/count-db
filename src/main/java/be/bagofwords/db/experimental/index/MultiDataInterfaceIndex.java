package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.data.ListCombinator;
import be.bagofwords.db.data.ListSerializer;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.MappedLists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiDataInterfaceIndex<T> extends BaseDataInterfaceIndex<T> {

    private final MultiDataIndexer<T> indexer;
    private final DataInterface<List<KeyValue<T>>> indexedDataInterface;

    public MultiDataInterfaceIndex(String name, DataInterfaceFactory dataInterfaceFactory, DataInterface<T> dataInterface, MultiDataIndexer<T> indexer) {
        super(dataInterface);
        this.indexer = indexer;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(name, List.class, new ListCombinator(dataInterface.getCombinator()), new ListSerializer(dataInterface.getObjectSerializer()));
    }

    @Override
    protected String getIndexName() {
        return indexedDataInterface.getName();
    }

    public List<KeyValue<T>> read(long indexKey) {
        return indexedDataInterface.read(indexKey);
    }

    public List<KeyValue<T>> read(T queryByObject) {
        return streamValues(queryByObject).collect(Collectors.toList());
    }

    public Stream<KeyValue<T>> streamValues(T queryByObject) {
        List<Long> indexKeys = indexer.convertToIndexes(queryByObject);
        return indexedDataInterface.streamValues(IterableUtils.iterator(indexKeys))
                .flatMap(Collection::stream)
                .distinct();
    }

    public Stream<KeyValue<T>> streamValues(KeyFilter keyFilter) {
        return indexedDataInterface.streamValues(keyFilter)
                .flatMap(Collection::stream)
                .distinct();
    }

    public Stream<KeyValue<T>> streamValues() {
        return indexedDataInterface.streamValues().flatMap(Collection::stream)
                .distinct();
    }

    public void close() {
        this.indexedDataInterface.close();
    }

    @Override
    public void dateUpdated(long key, T value) {
        List<Long> indexes = indexer.convertToIndexes(value);
        List<KeyValue<T>> singleton = Collections.singletonList(new KeyValue<>(key, value));
        for (Long index : indexes) {
            indexedDataInterface.write(index, singleton);
        }
    }

    @Override
    public void dateUpdated(List<KeyValue<T>> keyValues) {
        MappedLists<Long, KeyValue<T>> combinedValues = new MappedLists<>();
        for (KeyValue<T> keyValue : keyValues) {
            List<Long> indexes = indexer.convertToIndexes(keyValue.getValue());
            for (Long index : indexes) {
                combinedValues.get(index).add(keyValue);
            }
        }
        CloseableIterator<KeyValue<List<KeyValue<T>>>> iterator = IterableUtils.mapIterator(IterableUtils.iterator(combinedValues.entrySet()), entry -> new KeyValue<>(entry.getKey(), entry.getValue()));
        indexedDataInterface.write(iterator);
    }

    @Override
    public void dataFlushed() {
        indexedDataInterface.flush();
    }

    @Override
    public void dataDropped() {
        indexedDataInterface.dropAllData();
    }
}
