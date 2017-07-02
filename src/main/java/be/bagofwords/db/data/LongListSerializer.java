package be.bagofwords.db.data;

import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.db.methods.ReadValue;
import be.bagofwords.util.SerializationUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by koen on 1/07/17.
 */
public class LongListSerializer implements ObjectSerializer<LongList> {

    @Override
    public int writeValue(LongList obj, DataOutputStream dos) throws IOException {
        int length = obj.size() * 8;
        dos.writeInt(length);
        for (int i = 0; i < obj.size(); i++) {
            dos.writeLong(obj.get(i));
        }
        return 4 + length;
    }

    @Override
    public ReadValue<LongList> readValue(byte[] buffer, int position, boolean readActualValue) {
        int length = SerializationUtils.bytesToInt(buffer, position);
        LongList value;
        if (readActualValue) {
            int numOfItems = length / 8;
            value = new LongList();
            position += 4;
            for (int i = 0; i < numOfItems; i++) {
                value.add(SerializationUtils.bytesToLong(buffer, position));
                position += 8;
            }
        } else {
            value = null;
        }
        return new ReadValue<>(4 + length, value);
    }

    @Override
    public int getMinimumBoundOfObjectSize() {
        return 4;
    }

    @Override
    public int getValueWidth() {
        return -1;
    }
}
