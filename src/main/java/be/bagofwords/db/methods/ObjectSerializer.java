package be.bagofwords.db.methods;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Created by koen on 23/05/17.
 */
public interface ObjectSerializer<T> {

    int serialize(T t, OutputStream os) throws IOException;

    T deserialize(int length, InputStream is) throws IOException;

}
