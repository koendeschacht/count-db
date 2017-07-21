package be.bagofwords.db.data;

import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.util.KeyValue;

public class KeyValueSerializer<T> implements ObjectSerializer<KeyValue<T>> {

    private final ObjectSerializer<T> valueSerializer;

    public KeyValueSerializer(ObjectSerializer<T> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeValue(KeyValue<T> obj, DataStream ds) {
        ds.writeLong(obj.getKey());
        valueSerializer.writeValue(obj.getValue(), ds);
    }

    @Override
    public KeyValue<T> readValue(DataStream ds, int size) {
        long key = ds.readLong();
        T value = valueSerializer.readValue(ds, size - 8);
        return new KeyValue<>(key, value);
    }

    @Override
    public int getObjectSize() {
        return -1;
    }
}
