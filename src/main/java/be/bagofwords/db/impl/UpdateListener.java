package be.bagofwords.db.impl;

import be.bagofwords.util.KeyValue;

import java.util.List;

public interface UpdateListener<T> {

    void dateUpdated(long key, T value);

    void dateUpdated(List<KeyValue<T>> values);

    void dataFlushed();

    void dataDropped();

}
