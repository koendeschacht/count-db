package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.DataIterable;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.text.BowString;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.StreamUtils;

import java.util.Iterator;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Created by koen on 19/05/17.
 */
public interface DataInterface<T extends Object> extends DataIterable<KeyValue<T>> {

    T read(long key);

    default long readCount(long key) {
        Long result = (Long) read(key);
        if (result == null)
            return 0;
        else
            return result;
    }

    default T read(String key) {
        return read(HashUtils.hashCode(key));
    }

    default T read(BowString key) {
        return read(HashUtils.hashCode(key));
    }

    default long readCount(BowString key) {
        return readCount(HashUtils.hashCode(key));
    }

    default long readCount(String key) {
        return readCount(HashUtils.hashCode(key));
    }

    default boolean mightContain(String key) {
        return mightContain(HashUtils.hashCode(key));
    }

    default boolean mightContain(long key) {
        return read(key) != null;
    }

    default CloseableIterator<Long> keyIterator() {
        return IterableUtils.mapIterator(iterator(), KeyValue::getKey);
    }

    default CloseableIterator<T> valueIterator() {
        return IterableUtils.mapIterator(iterator(), KeyValue::getValue);
    }

    default CloseableIterator<T> valueIterator(KeyFilter keyFilter) {
        return IterableUtils.mapIterator(iterator(keyFilter), KeyValue::getValue);
    }

    default CloseableIterator<T> valueIterator(Predicate<T> valueFilter) {
        return IterableUtils.mapIterator(iterator(valueFilter), KeyValue::getValue);
    }

    default CloseableIterator<T> valueIterator(CloseableIterator<Long> keyIterator) {
        return IterableUtils.mapIterator(iterator(keyIterator), KeyValue::getValue);
    }

    default CloseableIterator<T> valueIterator(Stream<Long> keyStream) {
        return valueIterator(StreamUtils.iterator(keyStream));
    }

    default CloseableIterator<KeyValue<T>> iterator(KeyFilter keyFilter) {
        final CloseableIterator<KeyValue<T>> keyValueIterator = iterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {
            @Override
            public KeyValue<T> next() throws Exception {
                while (keyValueIterator.hasNext()) {
                    KeyValue<T> next = keyValueIterator.next();
                    if (keyFilter.acceptKey(next.getKey())) {
                        return next;
                    }
                }
                return null;
            }

            @Override
            public void close() throws Exception {
                keyValueIterator.close();
            }
        });
    }

    default CloseableIterator<KeyValue<T>> iterator(Predicate<T> valueFilter) {
        final CloseableIterator<KeyValue<T>> keyValueIterator = iterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {
            @Override
            public KeyValue<T> next() throws Exception {
                while (keyValueIterator.hasNext()) {
                    KeyValue<T> next = keyValueIterator.next();
                    if (valueFilter.test(next.getValue())) {
                        return next;
                    }
                }
                return null;
            }

            @Override
            public void close() throws Exception {
                keyValueIterator.close();
            }
        });
    }

    default CloseableIterator<KeyValue<T>> iterator(Stream<Long> keyStream) {
        return iterator(StreamUtils.iterator(keyStream));
    }

    default CloseableIterator<KeyValue<T>> iterator(CloseableIterator<Long> keyIterator) {
        return new CloseableIterator<KeyValue<T>>() {

            KeyValue<T> next;

            {
                //Constructor
                findNext();
            }

            private void findNext() {
                next = null;
                while (next == null && keyIterator.hasNext()) {
                    Long nextKey = keyIterator.next();
                    T nextValue = read(nextKey);
                    if (nextValue != null) {
                        next = new KeyValue<>(nextKey, nextValue);
                    }
                }
            }

            @Override
            protected void closeInt() {
                keyIterator.close();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> curr = next;
                findNext();
                return curr;
            }
        };
    }

    default CloseableIterator<KeyValue<T>> cachedValueIterator() {
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

    CloseableIterator<KeyValue<T>> iterator();

    long apprSize();

    long apprDataChecksum();

    long exactSize();

    default Stream<KeyValue<T>> stream() {
        return StreamUtils.stream(this, true);
    }

    default Stream<KeyValue<T>> stream(KeyFilter keyFilter) {
        return StreamUtils.stream(iterator(keyFilter), apprSize(), true);
    }

    default Stream<KeyValue<T>> stream(Predicate<T> valueFilter) {
        return StreamUtils.stream(iterator(valueFilter), apprSize(), true);
    }

    default Stream<T> streamValues() {
        return StreamUtils.stream(valueIterator(), apprSize(), false);
    }

    default Stream<T> streamValues(KeyFilter keyFilter) {
        return StreamUtils.stream(valueIterator(keyFilter), apprSize(), false);
    }

    default Stream<T> streamValues(Predicate<T> valueFilter) {
        return StreamUtils.stream(valueIterator(valueFilter), apprSize(), false);
    }

    default Stream<T> streamValues(CloseableIterator<Long> keyIterator) {
        return StreamUtils.stream(valueIterator(keyIterator), apprSize(), false);
    }

    default Stream<T> streamValues(Stream<Long> keysStream) {
        //This could be improved... Mapping twice from streams to closeable iterators does have a certain overhead
        return StreamUtils.stream(valueIterator(StreamUtils.iterator(keysStream)), apprSize(), false);
    }

    default Stream<Long> streamKeys() {
        return StreamUtils.stream(keyIterator(), apprSize(), true);
    }

    Class<T> getObjectClass();

    String getName();

    void optimizeForReading();

    void write(long key, T value);

    default void write(Iterator<KeyValue<T>> entries) {
        write(IterableUtils.iterator(entries));
    }

    default void write(CloseableIterator<KeyValue<T>> entries) {
        while (entries.hasNext()) {
            KeyValue<T> entry = entries.next();
            write(entry.getKey(), entry.getValue());
        }
        entries.close();
    }

    default void write(BowString key, T value) {
        write(HashUtils.hashCode(key.getS()), value);
    }

    default void write(String key, T value) {
        write(HashUtils.hashCode(key), value);
    }

    default void increaseCount(String key, Long value) {
        write(key, (T) value);
    }

    default void increaseCount(long key, Long value) {
        write(key, (T) value);
    }

    default void increaseCount(String key) {
        increaseCount(key, 1l);
    }

    default void increaseCount(long key) {
        increaseCount(key, 1l);
    }

    default void remove(String key) {
        remove(HashUtils.hashCode(key));
    }

    default void remove(long key) {
        write(key, null);
    }

    Combinator<T> getCombinator();

    void close();

    boolean wasClosed();

    boolean isTemporaryDataInterface();

    void flush();

    void dropAllData();

    void ifNotClosed(Runnable action);

    /**
     * Returns a number of the last flush, monotonically increasing
     */

    long lastFlush();

    DataInterface<T> getCoreDataInterface();

    ObjectSerializer<T> getObjectSerializer();

}
