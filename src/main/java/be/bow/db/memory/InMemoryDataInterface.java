package be.bow.db.memory;

import be.bow.db.Combinator;
import be.bow.db.CoreDataInterface;
import be.bow.db.DataInterface;
import be.bow.iterator.CloseableIterator;
import be.bow.iterator.IterableUtils;
import be.bow.util.KeyValue;

import java.util.*;

public class InMemoryDataInterface<T extends Object> extends CoreDataInterface<T> {

    private HashMap<Long, T> values;

    protected InMemoryDataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        super(name, objectClass, combinator);
        this.values = new HashMap<>();
    }

    @Override
    protected T readInt(long key) {
        synchronized (values) {
            return values.get(key);
        }
    }

    @Override
    protected void writeInt0(long key, T value) {
        synchronized (values) {
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
    }

    @Override
    protected void writeInt0(Iterator<KeyValue<T>> entries) {
        synchronized (values) {
            while (entries.hasNext()) {
                KeyValue<T> entry = entries.next();
                writeInt0(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
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
        values.clear();
    }

    @Override
    public void flush() {
        //do nothing
    }

    @Override
    public void close() {
        values = null;
    }

    @Override
    public DataInterface getImplementingDataInterface() {
        return null;
    }

    @Override
    public long apprSize() {
        return values.size();
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        List<Long> sortedKeys = new ArrayList<>(values.keySet());
        Collections.sort(sortedKeys);
        return IterableUtils.iterator(sortedKeys.iterator());
    }

    @Override
    public long exactSize() {
        return values.size();
    }

    @Override
    public boolean mightContain(long key) {
        return values.containsKey(key);
    }
}
