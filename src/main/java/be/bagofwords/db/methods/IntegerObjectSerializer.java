package be.bagofwords.db.methods;

import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by koen on 23/05/17.
 */
public class IntegerObjectSerializer implements ObjectSerializer<Long> {

    private final byte[] buffer = new byte[8];

    @Override
    public int serialize(Long t, OutputStream os) throws IOException {
        SerializationUtils.longToBytes(t, buffer, 0);
        os.write(buffer);
        return 8;
    }

    @Override
    public Long deserialize(int length, InputStream is) throws IOException {
        IOUtils.readFully(is, buffer);
        return SerializationUtils.bytesToLong(buffer);
    }

}
