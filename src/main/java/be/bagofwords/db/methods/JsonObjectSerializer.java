package be.bagofwords.db.methods;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by koen on 23/05/17.
 */
public class JsonObjectSerializer<T> implements ObjectSerializer<T> {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private final Class<T> _class;

    public JsonObjectSerializer(Class<T> _class) {
        this._class = _class;
    }

    @Override
    public int serialize(T t, OutputStream os) throws IOException {
        byte[] bytes = objectMapper.writeValueAsBytes(t);
        os.write(bytes);
        return bytes.length;
    }

    @Override
    public T deserialize(int length, InputStream is) throws IOException {
        byte[] bytes = new byte[length];
        IOUtils.readFully(is, bytes);
        return objectMapper.readValue(bytes, _class);
    }

}
