package be.bagofwords.db.experimental.rocksdb;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.DataLock;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.File;
import java.util.Iterator;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
public class RocksDBDataInterface<T> extends CoreDataInterface<T> {

    private final boolean useMergeHack;

    private final DataLock writeLock;
    private final WriteOptions delayedWriteOptions;
    private final File dbDirectory;
    private RocksDB db;

    public RocksDBDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, String directory, boolean usePatch) {
        super(name, objectClass, combinator);
        this.useMergeHack = usePatch;
        try {
            if (useMergeHack && objectClass == Long.class) {
                name = "_long_count_" + name;
            }
            dbDirectory = new File(new File(directory), name);
            if (dbDirectory.isFile()) {
                throw new RuntimeException(dbDirectory.getAbsolutePath() + " is a file, should be a directory...");
            } else if (!dbDirectory.exists()) {
                boolean success = dbDirectory.mkdirs();
                if (!success) {
                    throw new RuntimeException("Failed to create directory " + dbDirectory.getAbsolutePath());
                }
            }
            openDatabase();

        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to create database", e);
        }
        this.writeLock = new DataLock();
        this.delayedWriteOptions = new WriteOptions();
    }

    private void openDatabase() throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true);
        db = RocksDB.open(options, dbDirectory.getAbsolutePath());
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        writeLock.lockWriteAll();
        try {
            WriteBatch writeBatch = new WriteBatch();
            if (useMergeHack && getObjectClass() == Long.class) {
                while (entries.hasNext()) {
                    KeyValue<T> next = entries.next();
                    byte[] keyAsBytes = SerializationUtils.longToBytes(next.getKey());
                    if (next.getValue() == null) {
                        writeBatch.remove(keyAsBytes);
                    } else {
                        writeBatch.merge(keyAsBytes, SerializationUtils.objectToBytes(next.getValue(), getObjectClass()));
                    }
                }
            } else {
                while (entries.hasNext()) {
                    KeyValue<T> next = entries.next();
                    byte[] keyAsBytes = SerializationUtils.longToBytes(next.getKey());
                    T combinedValue = combineWithCurrentValue(next.getValue(), keyAsBytes);
                    if (combinedValue == null) {
                        writeBatch.remove(keyAsBytes);
                    } else {
                        writeBatch.put(keyAsBytes, SerializationUtils.objectToBytes(combinedValue, getObjectClass()));
                    }
                }
            }
            db.write(delayedWriteOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new RuntimeException("Received exception while trying to write multiple values to the DB", e);
        } finally {
            writeLock.unlockWriteAll();
        }
    }

    @Override
    public void write(long key, T value) {
        byte[] keyAsBytes = SerializationUtils.longToBytes(key);
        writeLock.lockWrite(key);
        try {
            T combinedValue = combineWithCurrentValue(value, keyAsBytes);
            if (combinedValue == null) {
                db.remove(delayedWriteOptions, keyAsBytes);
            } else {
                db.put(delayedWriteOptions, keyAsBytes, SerializationUtils.objectToBytes(combinedValue, getObjectClass()));
            }
        } catch (RocksDBException exp) {
            throw new RuntimeException("Received exception while trying to write a single value to the DB", exp);
        } finally {
            writeLock.unlockWrite(key);
        }
    }

    private T combineWithCurrentValue(T value, byte[] keyAsBytes) throws RocksDBException {
        T currentValue = SerializationUtils.bytesToObject(db.get(keyAsBytes), getObjectClass());
        T combinedValue;
        if (currentValue == null || value == null) {
            combinedValue = value;
        } else {
            combinedValue = getCombinator().combine(currentValue, value);
        }
        return combinedValue;
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        final RocksIterator rocksIterator = db.newIterator();
        rocksIterator.seekToFirst();
        return new CloseableIterator<KeyValue<T>>() {
            @Override
            protected void closeInt() {
                rocksIterator.dispose();
            }

            @Override
            public boolean hasNext() {
                return rocksIterator.isValid();
            }

            @Override
            public KeyValue<T> next() {
                long key = SerializationUtils.bytesToLong(rocksIterator.key());
                T value = SerializationUtils.bytesToObject(rocksIterator.value(), getObjectClass());
                rocksIterator.next();
                return new KeyValue<>(key, value);
            }
        };
    }

    @Override
    public void optimizeForReading() {
        //do nothing?
    }

    @Override
    public T read(long key) {
        writeLock.lockRead(key);
        try {
            T result = SerializationUtils.bytesToObject(db.get(SerializationUtils.longToBytes(key)), getObjectClass());
            return result;
        } catch (RocksDBException e) {
            throw new RuntimeException("Received exception while reading single value", e);
        } finally {
            writeLock.unlockRead(key);
        }
    }

    @Override
    public synchronized void dropAllData() {
        writeLock.lockWriteAll();
        db.close();
        try {
            FileUtils.deleteDirectory(dbDirectory);
            openDatabase();
        } catch (Exception exp) {
            throw new RuntimeException("Failed to drop all data", exp);
        }
        writeLock.unlockWriteAll();
    }

    @Override
    public void flush() {
        //OK
    }

    @Override
    public synchronized void doClose() {
        db.close();
        db = null;
    }

    @Override
    public long apprSize() {
        return exactSize();
    }
}
