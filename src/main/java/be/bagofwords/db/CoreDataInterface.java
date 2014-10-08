package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.util.KeyValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class CoreDataInterface<T> extends DataInterface<T> {

    private boolean wasClosed;

    public CoreDataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        super(name, objectClass, combinator);
        this.wasClosed = false;
    }

    @Override
    public void valuesChanged(long[] keys) {
        throw new RuntimeException("This method should not be called for core datainterface " + getClass());
    }

    @Override
    protected void writeInt(long key, T value) {
        writeInt0(key, value);
        notifyListenersOfChangedValues(new long[]{key});
    }

    @Override
    public void write(final Iterator<KeyValue<T>> entries) {
        final List<Long> keys = new ArrayList<>();
        writeInt0(new Iterator<KeyValue<T>>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> result = entries.next();
                keys.add(result.getKey());
                return result;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not supported");
            }
        });
        long[] keysAsArray = new long[keys.size()];
        for (int i = 0; i < keys.size(); i++) {
            keysAsArray[i] = keys.get(i);
        }
        notifyListenersOfChangedValues(keysAsArray);
    }

    protected abstract void writeInt0(Iterator<KeyValue<T>> entries);

    protected abstract void writeInt0(long key, T value);

    @Override
    protected DataInterface getImplementingDataInterface() {
        return null;
    }

    @Override
    public boolean wasClosed() {
        return wasClosed;
    }

    @Override
    public void close() {
        if (!wasClosed()) {
            doClose();
            wasClosed = true;
        }
    }

    protected abstract void doClose();
}
