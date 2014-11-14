package be.bagofwords.db;

import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;

import java.util.Iterator;

public abstract class LayeredDataInterface<T> extends DataInterface<T> {

    protected DataInterface<T> baseInterface;

    public LayeredDataInterface(DataInterface<T> baseInterface) {
        super(baseInterface.getName(), baseInterface.getObjectClass(), baseInterface.getCombinator(), baseInterface.isTemporaryDataInterface());
        this.baseInterface = baseInterface;
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
    public DataInterface getCoreDataInterface() {
        return baseInterface.getCoreDataInterface();
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

    protected final void doClose() {
        try {
            doCloseImpl();
        } finally {
            //even if the doCloseImpl() method failed, we still try to close the base interface
            baseInterface.close();
        }
    }

    @Override
    protected void requestClose() {
        super.requestClose();
        baseInterface.requestClose();
    }

    @Override
    public CloseableIterator<KeyValue<T>> cachedValueIterator() {
        return baseInterface.cachedValueIterator();
    }

    protected abstract void doCloseImpl();

}
