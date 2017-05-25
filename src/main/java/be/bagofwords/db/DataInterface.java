package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.DataIterable;
import be.bagofwords.text.BowString;
import be.bagofwords.util.KeyValue;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Created by koen on 19/05/17.
 */
public interface DataInterface<T extends Object> extends DataIterable<KeyValue<T>> {

    T read(long key);

    long readCount(long key);

    T read(String key);

    T read(BowString key);

    long readCount(BowString key);

    long readCount(String key);

    boolean mightContain(String key);

    boolean mightContain(long key);

    CloseableIterator<KeyValue<T>> iterator();

    CloseableIterator<KeyValue<T>> iterator(KeyFilter keyFilter);

    CloseableIterator<Long> keyIterator();

    CloseableIterator<T> valueIterator();

    CloseableIterator<T> valueIterator(KeyFilter keyFilter);

    CloseableIterator<KeyValue<T>> iterator(Iterator<Long> keyIterator);

    CloseableIterator<KeyValue<T>> cachedValueIterator();

    void optimizeForReading();

    long apprSize();

    void write(BowString key, T value);

    void write(String key, T value);

    void write(long key, T value);

    void increaseCount(String key, Long value);

    void increaseCount(long key, Long value);

    void increaseCount(String key);

    void increaseCount(long key);

    void remove(String key);

    void remove(long key);

    long apprDataChecksum();

    long exactSize();

    void write(Iterator<KeyValue<T>> entries);

    Stream<KeyValue<T>> stream();

    Stream<KeyValue<T>> stream(KeyFilter keyFilter);

    Stream<T> streamValues();

    Stream<T> streamValues(KeyFilter keyFilter);

    Stream<Long> streamKeys();

    Combinator<T> getCombinator();

    Class<T> getObjectClass();

    String getName();

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

}
