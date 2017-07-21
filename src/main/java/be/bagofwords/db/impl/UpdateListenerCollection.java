package be.bagofwords.db.impl;

import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.List;

public class UpdateListenerCollection<T> implements UpdateListener<T> {

    private final List<UpdateListener<T>> updateListeners = new ArrayList<>();

    public void registerUpdateListener(UpdateListener<T> updateListener) {
        updateListeners.add(updateListener);
    }

    @Override
    public void dateUpdated(long key, T value) {
        for (UpdateListener<T> updateListener : updateListeners) {
            updateListener.dateUpdated(key, value);
        }
    }

    @Override
    public void dateUpdated(List<KeyValue<T>> keyValues) {
        for (UpdateListener<T> updateListener : updateListeners) {
            updateListener.dateUpdated(keyValues);
        }
    }

    @Override
    public void dataFlushed() {
        for (UpdateListener<T> updateListener : updateListeners) {
            updateListener.dataFlushed();
        }
    }

    @Override
    public void dataDropped() {
        for (UpdateListener<T> updateListener : updateListeners) {
            updateListener.dataDropped();
        }
    }
}
