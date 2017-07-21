package be.bagofwords.db.methods;

import be.bagofwords.exec.RemoteObjectConfig;

import java.io.Serializable;

/**
 * Created by koen on 23/05/17.
 */
public interface ObjectSerializer<T> extends Serializable {

    void writeValue(T obj, DataStream ds);

    T readValue(DataStream ds, int size);

    int getObjectSize();

    default RemoteObjectConfig createExecConfig() {
        return RemoteObjectConfig.create(this).add(getClass());
    }
}
