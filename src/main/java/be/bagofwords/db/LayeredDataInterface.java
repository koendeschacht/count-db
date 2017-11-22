package be.bagofwords.db;

import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.UpdateListener;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;

import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class LayeredDataInterface<T> extends BaseDataInterface<T> {

    protected DataInterface<T> baseInterface;

    public LayeredDataInterface(DataInterface<T> baseInterface) {
        super(baseInterface.getName(), baseInterface.getObjectClass(), baseInterface.getCombinator(), baseInterface.getObjectSerializer(), baseInterface.isTemporaryDataInterface());
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
    public DataInterface<T> getCoreDataInterface() {
        return baseInterface.getCoreDataInterface();
    }

    @Override
    public void registerUpdateListener(UpdateListener<T> updateListener) {
        baseInterface.registerUpdateListener(updateListener);
    }

    @Override
    public long apprSize() {
        return baseInterface.apprSize();
    }

    @Override
    public void write(CloseableIterator<KeyValue<T>> entries) {
        baseInterface.write(entries);
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(CloseableIterator<Long> keyIterator) {
        return baseInterface.iterator(keyIterator);
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(Predicate<T> valueFilter) {
        return baseInterface.iterator(valueFilter);
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(KeyFilter keyFilter) {
        return baseInterface.iterator(keyFilter);
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        return baseInterface.keyIterator();
    }

    @Override
    public CloseableIterator<T> valueIterator() {
        return baseInterface.valueIterator();
    }

    @Override
    public CloseableIterator<T> valueIterator(KeyFilter keyFilter) {
        return baseInterface.valueIterator(keyFilter);
    }

    @Override
    public CloseableIterator<T> valueIterator(CloseableIterator<Long> keyIterator) {
        return baseInterface.valueIterator(keyIterator);
    }

    @Override
    public Stream<T> streamValues(KeyFilter keyFilter) {
        return baseInterface.streamValues(keyFilter);
    }

    @Override
    public Stream<T> streamValues(Predicate<T> valueFilter) {
        return baseInterface.streamValues(valueFilter);
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
    public CloseableIterator<KeyValue<T>> cachedValueIterator() {
        return baseInterface.cachedValueIterator();
    }

    protected abstract void doCloseImpl();

}
