package be.bagofwords.db.methods;

import be.bagofwords.logging.Log;
import be.bagofwords.util.SerializationUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by koen on 23/05/17.
 */
public class LongObjectSerializer implements ObjectSerializer<Long> {

    private final long NULL_VALUE = Long.MIN_VALUE;

    @Override
    public int writeValue(Long obj, DataOutputStream dos) throws IOException {
        if (obj == null) {
            obj = NULL_VALUE;
        } else if (obj == NULL_VALUE) {
            throw new RuntimeException("Sorry, value " + obj + " is a reserved value");
        }
        dos.writeLong(obj);
        return 8;
    }

    @Override
    public ReadValue<Long> readValue(byte[] buffer, int position, boolean readActualValue) {
        Long value;
        if (readActualValue) {
            value = SerializationUtils.bytesToLong(buffer, position);
            if (value == NULL_VALUE) {
                value = null;
            }
        } else {
            value = null;
        }
        return new ReadValue<>(8, value);
    }

    @Override
    public int getMinimumBoundOfObjectSize() {
        return 8;
    }

    @Override
    public int getValueWidth() {
        return 8;
    }
}
