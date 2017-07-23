package be.bagofwords.db.methods;

import be.bagofwords.exec.RemoteClass;

/**
 * Created by koen on 1/07/17.
 */
@RemoteClass
public class StringSerializer implements ObjectSerializer<String> {

    @Override
    public void writeValue(String obj, DataStream ds) {
        ds.writeString(obj, false);
    }

    @Override
    public String readValue(DataStream ds, int size) {
        return ds.readString(size);
    }

    @Override
    public int getObjectSize() {
        return -1;
    }

}

