package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.data.KeyValueSerializer;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.StreamUtils;

import java.util.List;
import java.util.stream.Stream;

public class UniqueDataInterfaceIndex<T> extends BaseDataInterfaceIndex<T> {

    private final UniqueDataIndexer<T> indexer;
    private final BaseDataInterface<KeyValue<T>> indexedDataInterface;

    public UniqueDataInterfaceIndex(String name, DataInterfaceFactory dataInterfaceFactory, DataInterface<T> dataInterface, UniqueDataIndexer<T> indexer) {
        super(dataInterface);
        this.indexer = indexer;
        this.indexedDataInterface = dataInterfaceFactory.createDataInterface(name, (Class<KeyValue<T>>) (Object) KeyValue.class, new UniqueKeyCombinator(), new KeyValueSerializer<T>(dataInterface.getObjectSerializer()));
    }

    @Override
    protected String getIndexName() {
        return indexedDataInterface.getName();
    }

    public KeyValue<T> read(long indexKey) {
        return indexedDataInterface.read(indexKey);
    }

    public KeyValue<T> read(T queryByObject) {
        long indexKey = indexer.convertToIndex(queryByObject);
        return read(indexKey);
    }

    public Stream<KeyValue<T>> streamValues(KeyFilter keyFilter) {
        return indexedDataInterface.streamValues(keyFilter);
    }

    public Stream<KeyValue<T>> streamValues() {
        return indexedDataInterface.streamValues();
    }

    public void close() {
        this.indexedDataInterface.close();
    }

    @Override
    public void dateUpdated(long key, T value) {
        long index = indexer.convertToIndex(value);
        indexedDataInterface.write(index, new KeyValue<>(key, value));
    }

    @Override
    public void dateUpdated(List<KeyValue<T>> keyValues) {
        Stream<KeyValue<KeyValue<T>>> stream = keyValues.stream().map(kv -> new KeyValue<>(indexer.convertToIndex(kv.getValue()), kv));
        indexedDataInterface.write(StreamUtils.iterator(stream));
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
