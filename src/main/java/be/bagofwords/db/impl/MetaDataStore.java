package be.bagofwords.db.impl;

import be.bagofwords.db.DataInterface;
import be.bagofwords.util.HashUtils;

/**
 * Created by koen on 19/05/17.
 */
public class MetaDataStore {

    private DataInterface<String> metaDataStorage;

    public String getString(DataInterface dataInterface, String propertyName) {
        return metaDataStorage.read(compositeKey(dataInterface, propertyName));
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

    public void setStorage(BaseDataInterface<String> metaStoreInterface) {
        this.metaDataStorage = metaStoreInterface;
    }
}
