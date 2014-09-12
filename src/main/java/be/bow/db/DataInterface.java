package be.bow.db;


import be.bow.db.combinator.Combinator;
import be.bow.iterator.CloseableIterator;
import be.bow.iterator.DataIterable;
import be.bow.iterator.IterableUtils;
import be.bow.iterator.SimpleIterator;
import be.bow.text.SimpleString;
import be.bow.util.HashUtils;
import be.bow.util.KeyValue;
import be.bow.util.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class DataInterface<T extends Object> implements DataIterable<KeyValue<T>>, ChangedValuesPublisher, ChangedValuesListener {

    private final Combinator<T> combinator;
    private final Class<T> objectClass;
    private final String name;
    protected long numberOfWrites;
    protected long numberOfReads;
    protected long totalTimeRead;
    protected long totalTimeWrite;
    private final List<ChangedValuesListener> listeners;

    protected DataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }
        this.name = name;
        this.objectClass = objectClass;
        this.combinator = combinator;
        listeners = new ArrayList<>();
    }

    public T read(long key) {
        numberOfReads++;
        long start = System.currentTimeMillis();
        T value = readInt(key);
        totalTimeRead += System.currentTimeMillis() - start;
        return value;
    }

    public T read(String key) {
        return read(HashUtils.hashCode(key));
    }

    public T read(SimpleString key) {
        return read(HashUtils.hashCode(key));
    }

    public long readCount(long key) {
        Long result = (Long) read(key);
        if (result == null)
            return 0;
        else
            return result;
    }

    public long readCount(SimpleString key) {
        return readCount(HashUtils.hashCode(key));
    }

    public long readCount(String key) {
        return readCount(HashUtils.hashCode(key));
    }

    public boolean mightContain(String key) {
        return mightContain(HashUtils.hashCode(key));
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    public boolean mightContain(long key) {
        return read(key) != null;
    }

    public abstract CloseableIterator<KeyValue<T>> iterator();

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    public CloseableIterator<Long> keyIterator() {
        final CloseableIterator<KeyValue<T>> keyValueIterator = iterator();
        return new CloseableIterator<Long>() {
            @Override
            public boolean hasNext() {
                return keyValueIterator.hasNext();
            }

            @Override
            public Long next() {
                return keyValueIterator.next().getKey();
            }

            @Override
            public void closeInt() {
                keyValueIterator.close();
            }
        };
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    public CloseableIterator<T> valueIterator() {
        final CloseableIterator<KeyValue<T>> keyValueIterator = iterator();
        return new CloseableIterator<T>() {
            @Override
            public boolean hasNext() {
                return keyValueIterator.hasNext();
            }

            @Override
            public T next() {
                return keyValueIterator.next().getValue();
            }

            @Override
            public void closeInt() {
                keyValueIterator.close();
            }
        };
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {
            @Override
            public KeyValue<T> next() throws Exception {
                while (keyIterator.hasNext()) {
                    Long next = keyIterator.next();
                    T value = read(next);
                    if (value != null) {
                        return new KeyValue<>(next, value);
                    }
                }
                return null;
            }
        });
    }

    protected abstract T readInt(long key);


    protected abstract void writeInt(long key, T value);

    public abstract void dropAllData();

    public abstract void flush();

    public abstract void close();

    public abstract long apprSize();

    public Combinator<T> getCombinator() {
        return combinator;
    }

    protected abstract DataInterface getImplementingDataInterface();

    public Class<T> getObjectClass() {
        return objectClass;
    }


    public void write(SimpleString key, T value) {
        write(HashUtils.hashCode(key.getS()), value);
    }

    public void write(String key, T value) {
        write(HashUtils.hashCode(key), value);
    }

    public void write(long key, T value) {
        numberOfWrites++;
        long start = System.currentTimeMillis();
        writeInt(key, value);
        totalTimeWrite += System.currentTimeMillis() - start;
    }

    public void increaseCount(String key, Long value) {
        write(key, (T) value);
    }

    public void increaseCount(long key, Long value) {
        write(key, (T) value);
    }

    public void increaseCount(String key) {
        increaseCount(key, 1l);
    }

    public void increaseCount(long key) {
        increaseCount(key, 1l);
    }

    public String getName() {
        return name;
    }

    public void remove(String key) {
        remove(HashUtils.hashCode(key));
    }

    public void remove(long key) {
        write(key, null);
    }


    public long dataCheckSum() {
        CloseableIterator<KeyValue<T>> valueIterator = iterator();
        final int numToSample = 10000;
        long checksum = 0;
        int numDone = 0;
        while (valueIterator.hasNext() && numDone < numToSample) {
            KeyValue<T> next = valueIterator.next();
            if (next.getValue() == null) {
                throw new RuntimeException("Iterating over values returned null for key " + next.getKey());
            }
            T value = next.getValue();
            checksum += checksum * 31 + value.hashCode();
            numDone++;
        }
        valueIterator.close();
        return checksum;
    }

    /**
     * You don't want to use this if you could use apprSize()
     */

    public long exactSize() {
        long result = 0;
        CloseableIterator<Long> keyIt = keyIterator();
        while (keyIt.hasNext()) {
            keyIt.next();
            result++;
        }
        keyIt.close();
        return result;
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    public void write(Iterator<KeyValue<T>> entries) {
        while (entries.hasNext()) {
            KeyValue<T> entry = entries.next();
            write(entry.getKey(), entry.getValue());
        }
    }

    public long getNumberOfWrites() {
        return numberOfWrites;
    }

    public long getNumberOfReads() {
        return numberOfReads;
    }

    public long getTotalTimeRead() {
        return totalTimeRead;
    }

    public long getTotalTimeWrite() {
        return totalTimeWrite;
    }

    @Override
    public synchronized void registerListener(ChangedValuesListener listener) {
        listeners.add(listener);
    }

    public void notifyListenersOfChangedValues(long[] keys) {
        for (ChangedValuesListener listener : listeners) {
            listener.valuesChanged(keys);
        }
    }
}
