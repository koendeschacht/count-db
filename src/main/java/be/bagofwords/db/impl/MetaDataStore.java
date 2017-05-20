package be.bagofwords.db.impl;

import be.bagofwords.db.DataInterface;
import be.bagofwords.util.HashUtils;

/**
 * Created by koen on 19/05/17.
 */
public class MetaDataStore {

    private DataInterface<String> metaDataStorage;

    public String getString(DataInterface dataInterface, String propertyName) {
        metaDataStorage.flush();
        return metaDataStorage.read(compositeKey(dataInterface, propertyName));
    }

    public long getLong(DataInterface dataInterface, String propertyName, long defaultValue) {
        Long value = getLong(dataInterface, propertyName);
        if (value == null) {
            return defaultValue;
        } else {
            return value;
        }
    }

    public Long getLong(DataInterface dataInterface, String propertyName) {
        String longAsString = getString(dataInterface, propertyName);
        if (longAsString == null) {
            return null;
        } else {
            return Long.parseLong(longAsString);
        }
    }

    private long compositeKey(DataInterface dataInterface, String propertyName) {
        return HashUtils.hashCode(dataInterface.getName(), propertyName);
    }

    public void setStorage(DataInterface<String> metaStoreInterface) {
        this.metaDataStorage = metaStoreInterface;
    }

    public void write(DataInterface dataInterface, String propertyName, String value) {
        metaDataStorage.write(compositeKey(dataInterface, propertyName), value);
    }

    public void write(DataInterface dataInterface, String propertyName, long value) {
        write(dataInterface, propertyName, Long.toString(value));
    }

    public void close() {
        if (metaDataStorage != null) {
            metaDataStorage.close();
        }
    }

    public boolean hasStorage() {
        return metaDataStorage != null;
    }
}
