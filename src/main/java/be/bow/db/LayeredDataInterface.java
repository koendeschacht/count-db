package be.bow.db;

import be.bow.iterator.CloseableIterator;
import be.bow.util.KeyValue;

import java.util.Iterator;

public class LayeredDataInterface<T> extends DataInterface<T> {

    protected DataInterface<T> baseInterface;

    public LayeredDataInterface(DataInterface<T> baseInterface) {
        super(baseInterface.getName(), baseInterface.getObjectClass(), baseInterface.getCombinator());
        this.baseInterface = baseInterface;
        this.baseInterface.registerListener(this);
    }

    @Override
    protected T readInt(long key) {
        return baseInterface.read(key);
    }

    @Override
    protected void writeInt(long key, T value) {
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
    public void close() {
        baseInterface.close();
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

    public CloseableIterator<KeyValue<T>> read(final Iterator<Long> keyIterator) {
        return baseInterface.read(keyIterator);
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
}
