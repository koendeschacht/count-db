package be.bagofwords.db.methods;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by koen on 23/05/17.
 */
public interface ObjectSerializer<T> {

    int writeValue(T obj, DataOutputStream dos) throws IOException;

    ReadValue<T> readValue(byte[] buffer, int position, boolean readActualValue);

    int getMinimumBoundOfObjectSize();

    int getValueWidth();
}
