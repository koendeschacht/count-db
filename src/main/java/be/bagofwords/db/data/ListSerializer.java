package be.bagofwords.db.data;

import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.DataStreamUtils;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class ListSerializer<T> implements ObjectSerializer<List<KeyValue<T>>> {

    private final ObjectSerializer<T> valueSerializer;

    public ListSerializer(ObjectSerializer<T> valueSerializer) {
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeValue(List<KeyValue<T>> values, DataStream ds) {
        ds.writeInt(values.size());
        for (KeyValue<T> value : values) {
            ds.writeLong(value.getKey());
            DataStreamUtils.writeValue(value.getValue(), ds, valueSerializer);
        }
    }

    @Override
    public List<KeyValue<T>> readValue(DataStream ds, int size) {
        int numOfItems = ds.readInt();
        List<KeyValue<T>> result = new ArrayList<>(numOfItems);
        for (int i = 0; i < numOfItems; i++) {
            long key = ds.readLong();
            int itemSize = DataStreamUtils.getObjectSize(ds, valueSerializer);
            T readValue = valueSerializer.readValue(ds, itemSize);
            result.add(new KeyValue<>(key, readValue));
        }
        return result;
    }

    @Override
    public int getObjectSize() {
        return -1;
    }
}
