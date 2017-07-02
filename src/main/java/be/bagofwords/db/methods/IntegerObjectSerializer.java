package be.bagofwords.db.methods;

import be.bagofwords.util.SerializationUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by koen on 23/05/17.
 */
public class IntegerObjectSerializer implements ObjectSerializer<Integer> {

    private final byte[] buffer = new byte[4];

    @Override
    public int writeValue(Integer obj, DataOutputStream dos) throws IOException {
        dos.writeInt(obj);
        return 4;
    }

    @Override
    public ReadValue<Integer> readValue(byte[] buffer, int position, boolean readActualValue) {
        Integer value;
        if (readActualValue) {
            value = SerializationUtils.bytesToInt(buffer, position);
        } else {
            value = null;
        }
        return new ReadValue<>(4, value);
    }

    @Override
    public int getMinimumBoundOfObjectSize() {
        return 4;
    }

    @Override
    public int getValueWidth() {
        return 4;
    }
}
