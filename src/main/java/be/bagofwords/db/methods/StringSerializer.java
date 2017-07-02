package be.bagofwords.db.methods;

import be.bagofwords.util.SerializationUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by koen on 1/07/17.
 */
public class StringSerializer implements ObjectSerializer<String> {

    private static final String NULL_VALUE = "xxx_NULL";

    @Override
    public int writeValue(String obj, DataOutputStream dos) throws IOException {
        return writeStringValue(obj, dos);
    }

    @Override
    public ReadValue<String> readValue(byte[] buffer, int position, boolean readActualValue) {
        return readStringValue(buffer, position, readActualValue);
    }

    @Override
    public int getMinimumBoundOfObjectSize() {
        return 4;
    }

    @Override
    public int getValueWidth() {
        return -1;
    }

    public static ReadValue<String> readStringValue(byte[] buffer, int position, boolean readActualValue) {
        int length = SerializationUtils.bytesToInt(buffer, position);
        String value;
        if (readActualValue) {
            value = new String(buffer, position + 4, length);
            if (value.equals(NULL_VALUE)) {
                value = null;
            }
        } else {
            value = null;
        }
        return new ReadValue<>(4 + length, value);
    }

    public static int writeStringValue(String obj, DataOutputStream dos) throws IOException {
        if (obj == null) {
            obj = NULL_VALUE;
        }
        byte[] bytes = obj.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
        return 4 + bytes.length;
    }
}

