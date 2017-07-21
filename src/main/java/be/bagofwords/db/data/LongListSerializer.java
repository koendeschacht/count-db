package be.bagofwords.db.data;

import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.ObjectSerializer;

/**
 * Created by koen on 1/07/17.
 */
public class LongListSerializer implements ObjectSerializer<LongList> {

    @Override
    public void writeValue(LongList obj, DataStream ds) {
        for (int i = 0; i < obj.size(); i++) {
            ds.writeLong(obj.get(i));
        }
    }

    @Override
    public LongList readValue(DataStream ds, int size) {
        int numOfItems = size / 8;
        LongList result = new LongList();
        for (int i = 0; i < numOfItems; i++) {
            result.add(ds.readLong());
        }
        return result;
    }

    @Override
    public int getObjectSize() {
        return -1;
    }
}
