package be.bagofwords.db;

import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;

import java.util.Iterator;

public abstract class LayeredDataInterface<T> extends DataInterface<T> {

    protected DataInterface<T> baseInterface;

    public LayeredDataInterface(DataInterface<T> baseInterface) {
        super(baseInterface.getName(), baseInterface.getObjectClass(), baseInterface.getCombinator());
        this.baseInterface = baseInterface;
        this.baseInterface.registerListener(this);
    }

    @Override
    public T read(long key) {
        return baseInterface.read(key);
    }

    @Override
    public void write(long key, T value) {
        baseInterface.write(key, value);
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        return baseInterface.iterator();
    }

    @Override
    public void dropAllData() {
        baseInterface.dropAllData();
    }

    @Override
    public void flush() {
        baseInterface.flush();
    }

    @Override
    public void optimizeForReading() {
        flush();
        baseInterface.optimizeForReading();
    }

    @Override
    public DataInterface getImplementingDataInterface() {
        return baseInterface;
    }

    @Override
    public long apprSize() {
        return baseInterface.apprSize();
    }

    public void write(Iterator<KeyValue<T>> entries) {
        baseInterface.write(entries);
    }

    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        return baseInterface.iterator(keyIterator);
    }

    public CloseableIterator<Long> keyIterator() {
        return baseInterface.keyIterator();
    }

    public CloseableIterator<T> valueIterator() {
        return baseInterface.valueIterator();
    }

    public boolean mightContain(long key) {
        return baseInterface.mightContain(key);
    }

    @Override
    public void valuesChanged(long[] keys) {
        notifyListenersOfChangedValues(keys); //pass on to listeners
    }

    @Override
    public void close() {
        if (!wasClosed()) {
            try {
                doClose();
            } finally {
                //even if the doClose() method failed, we still try to close the base interface
                baseInterface.close();
            }
        }
    }

    @Override
    public boolean wasClosed() {
        return baseInterface.wasClosed();
    }

    protected abstract void doClose();
}
