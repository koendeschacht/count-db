package be.bagofwords.db.memory;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.util.DataLock;
import be.bagofwords.util.KeyValue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryDataInterface<T extends Object> extends CoreDataInterface<T> {

    private Map<Long, T> values;
    private final DataLock lock;

    protected InMemoryDataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        super(name, objectClass, combinator);
        this.values = new ConcurrentHashMap<>();
        this.lock = new DataLock();
    }

    @Override
    public T read(long key) {
        return values.get(key);
    }

    @Override
    protected void writeInt0(long key, T value) {
        lock.lockWrite(key);
        nonSynchronizedWrite(key, value);
        lock.unlockWrite(key);
    }

    private void nonSynchronizedWrite(long key, T value) {
        if (value == null) {
            values.remove(key);
        } else {
            T currentValue = values.get(key);
            if (currentValue == null) {
                values.put(key, value);
            } else {
                values.put(key, getCombinator().combine(currentValue, value));
            }
        }
    }

    @Override
    protected void writeInt0(Iterator<KeyValue<T>> entries) {
        lock.lockWriteAll();
        while (entries.hasNext()) {
            KeyValue<T> entry = entries.next();
            nonSynchronizedWrite(entry.getKey(), entry.getValue());
        }
        lock.unlockWriteAll();
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        //We should probably add locking for this iterator, but do we want to
        //keep all the data locked until it is closed?
        List<Map.Entry<Long, T>> sortedValues = new ArrayList<>(values.entrySet());
        Collections.sort(sortedValues, new Comparator<Map.Entry<Long, T>>() {
            @Override
            public int compare(Map.Entry<Long, T> o1, Map.Entry<Long, T> o2) {
                return Long.compare(o1.getKey(), o2.getKey());
            }
        });
        final Iterator<Map.Entry<Long, T>> valuesIt = sortedValues.iterator();
        return new CloseableIterator<KeyValue<T>>() {
            @Override
            public boolean hasNext() {
                return valuesIt.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                Map.Entry<Long, T> next = valuesIt.next();
                return new KeyValue<>(next.getKey(), next.getValue());
            }

            @Override
            public void remove() {
                valuesIt.remove();
            }

            @Override
            public void closeInt() {
                //ok
            }
        };
    }

    @Override
    public void dropAllData() {
        lock.lockWriteAll();
        values.clear();
        lock.unlockWriteAll();
    }

    @Override
    public void flush() {
        //make sure that all writes have completely finished:
        lock.lockWriteAll();
        lock.unlockWriteAll();
    }

    @Override
    protected void doClose() {
        lock.lockWriteAll();
        values = null;
        lock.unlockWriteAll();
    }

    @Override
    public long apprSize() {
        return values.size();  //no locking needed since it is only the approximate size
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        lock.lockReadAll();
        List<Long> sortedKeys = new ArrayList<>(values.keySet());
        lock.unlockReadAll();
        Collections.sort(sortedKeys);
        return IterableUtils.iterator(sortedKeys.iterator());
    }

    @Override
    public void optimizeForReading() {
        //do nothing
    }

    @Override
    public long exactSize() {
        lock.lockReadAll();
        long result = values.size();
        lock.unlockReadAll();
        return result;
    }

    @Override
    public boolean mightContain(long key) {
        lock.lockRead(key);
        boolean result = values.containsKey(key);
        lock.unlockRead(key);
        return result;
    }
}
