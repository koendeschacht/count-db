package be.bagofwords.db.methods;

import be.bagofwords.util.SerializationUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by koen on 23/05/17.
 */
public class JsonObjectSerializer<T> implements ObjectSerializer<T> {

    private final Class<T> _class;

    public JsonObjectSerializer(Class<T> _class) {
        this._class = _class;
    }

    @Override
    public int writeValue(T obj, DataOutputStream dos) throws IOException {
        byte[] bytes = SerializationUtils.objectToBytesCheckForNull(obj, _class);
        dos.writeInt(bytes.length);
        dos.write(bytes);
        return bytes.length + 4;
    }

    @Override
    public ReadValue<T> readValue(byte[] buffer, int position, boolean readActualValue) {
        int length = SerializationUtils.bytesToInt(buffer, position);
        T value;
        if (readActualValue) {
            value = SerializationUtils.bytesToObjectCheckForNull(buffer, position + 4, length, _class);
        } else {
            value = null;
        }
        return new ReadValue<>(length + 4, value);
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
