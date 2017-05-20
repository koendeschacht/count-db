package be.bagofwords.db.impl;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.text.BowString;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.StreamUtils;
import be.bagofwords.util.StringUtils;

import java.util.Iterator;
import java.util.stream.Stream;

public abstract class BaseDataInterface<T extends Object> implements DataInterface<T> {

    protected final Combinator<T> combinator;
    protected final Class<T> objectClass;
    protected final String name;
    protected final boolean isTemporaryDataInterface;
    private final Object closeLock = new Object();
    private boolean wasClosed;

    public BaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }
        this.objectClass = objectClass;
        this.name = name;
        this.isTemporaryDataInterface = isTemporaryDataInterface;
        this.combinator = combinator;
    }

    public abstract DataInterface<T> getCoreDataInterface();

    protected abstract void doClose();

    public Combinator<T> getCombinator() {
        return combinator;
    }

    @Override
    public long readCount(long key) {
        Long result = (Long) read(key);
        if (result == null)
            return 0;
        else
            return result;
    }

    public Class<T> getObjectClass() {
        return objectClass;
    }

    public String getName() {
        return name;
    }

    public final void close() {
        ifNotClosed(() -> {
                    Log.i("Closing " + getName());
                    if (isTemporaryDataInterface) {
                        dropAllData();
                    }
                    flush();
                    doClose();
                    wasClosed = true;
                    Log.i("Closed " + getName());
                }
        );
    }

    public final boolean wasClosed() {
        return wasClosed;
    }

    @Override
    protected void finalize() throws Throwable {
        ifNotClosed(() -> {
            if (!isTemporaryDataInterface()) {
                //the user did not close the data interface himself?
                Log.i("Closing data interface " + getName() + " because it is about to be garbage collected.");
            }
            close();
        });
        super.finalize();
    }

    public void ifNotClosed(Runnable action) {
        synchronized (closeLock) {
            if (!wasClosed()) {
                action.run();
            }
        }
    }

    public boolean isTemporaryDataInterface() {
        return isTemporaryDataInterface;
    }

    @Override
    public T read(String key) {
        return read(HashUtils.hashCode(key));
    }

    @Override
    public T read(BowString key) {
        return read(HashUtils.hashCode(key));
    }

    @Override
    public long readCount(BowString key) {
        return readCount(HashUtils.hashCode(key));
    }

    @Override
    public long readCount(String key) {
        return readCount(HashUtils.hashCode(key));
    }

    @Override
    public boolean mightContain(String key) {
        return mightContain(HashUtils.hashCode(key));
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    @Override
    public boolean mightContain(long key) {
        return read(key) != null;
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    @Override
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

    @Override
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

    @Override
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

    @Override
    public CloseableIterator<KeyValue<T>> cachedValueIterator() {
        return new CloseableIterator<KeyValue<T>>() {
            @Override
            protected void closeInt() {
                //ok
            }

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public KeyValue<T> next() {
                return null;
            }
        };
    }

    @Override
    public void write(BowString key, T value) {
        write(HashUtils.hashCode(key.getS()), value);
    }

    @Override
    public void write(String key, T value) {
        write(HashUtils.hashCode(key), value);
    }

    @Override
    public void increaseCount(String key, Long value) {
        write(key, (T) value);
    }

    @Override
    public void increaseCount(long key, Long value) {
        write(key, (T) value);
    }

    @Override
    public void increaseCount(String key) {
        increaseCount(key, 1l);
    }

    @Override
    public void increaseCount(long key) {
        increaseCount(key, 1l);
    }

    @Override
    public void remove(String key) {
        remove(HashUtils.hashCode(key));
    }

    @Override
    public void remove(long key) {
        write(key, null);
    }

    @Override
    public long apprDataChecksum() {
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

    @Override
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

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        while (entries.hasNext()) {
            KeyValue<T> entry = entries.next();
            write(entry.getKey(), entry.getValue());
        }
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    @Override
    public Stream<KeyValue<T>> stream() {
        return StreamUtils.stream(this, true);
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    @Override
    public Stream<T> streamValues() {
        return StreamUtils.stream(valueIterator(), apprSize(), false);
    }

    /**
     * This method can be overwritten in a subclass to improve efficiency
     */

    @Override
    public Stream<Long> streamKeys() {
        return StreamUtils.stream(keyIterator(), apprSize(), true);
    }

}
