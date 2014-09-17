package be.bow.db.leveldb;

import be.bow.db.CoreDataInterface;
import be.bow.db.combinator.Combinator;
import be.bow.iterator.CloseableIterator;
import be.bow.util.DataLock;
import be.bow.util.KeyValue;
import be.bow.util.SerializationUtils;
import org.fusesource.leveldbjni.internal.JniDB;
import org.fusesource.leveldbjni.internal.JniKeyDBIterator;
import org.iq80.leveldb.*;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

public class LevelDBDataInterface<T> extends CoreDataInterface<T> {

    private DB db;
    private File databaseDir;
    private DataLock dataLock;

    public LevelDBDataInterface(String directory, String name, Class<T> objectClass, Combinator<T> combinator) {
        super(name, objectClass, combinator);
        try {
            databaseDir = new File(directory + File.separator + name);
            if (!databaseDir.exists()) {
                databaseDir.mkdirs();
            }
            db = factory.open(databaseDir, createOptions());
        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
        dataLock = new DataLock();
    }

    private Options createOptions() {
        Options options = new Options();
        options.createIfMissing(true);
        options.cacheSize(20 * 1024 * 1024);
        return options;
    }

    @Override
    public void optimizeForReading() {
        db.compactRange(null, null);
    }

    @Override
    protected T readInt(long key) {
        return SerializationUtils.bytesToObject(db.get(SerializationUtils.longToBytes(key)), getObjectClass());
    }

    @Override
    protected void writeInt0(long key, T value) {
        dataLock.lockWrite(key);
        if (value == null) {
            db.delete(SerializationUtils.longToBytes(key));
        } else {
            byte[] keyInBytes = SerializationUtils.longToBytes(key);
            byte[] currentValueInBytes = db.get(keyInBytes);
            T valueToWrite;
            if (currentValueInBytes == null) {
                valueToWrite = value;
            } else {
                valueToWrite = getCombinator().combine(SerializationUtils.bytesToObject(currentValueInBytes, getObjectClass()), value);
            }
            db.put(keyInBytes, SerializationUtils.objectToBytes(valueToWrite));
        }
        dataLock.unlockWrite(key);
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        final JniKeyDBIterator iterator = ((JniDB) db).keyIterator(new ReadOptions());
        iterator.seekToFirst();
        return new CloseableIterator<Long>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Long next() {
                byte[] entry = iterator.next();
                return SerializationUtils.bytesToLong(entry);
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not implemented");
            }


            @Override
            public void closeInt() {
                iterator.close();
            }
        };
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        final DBIterator iterator = db.iterator();
        iterator.seekToFirst();
        return new CloseableIterator<KeyValue<T>>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                Map.Entry<byte[], byte[]> entry = iterator.next();
                return new KeyValue<>(SerializationUtils.bytesToLong(entry.getKey()), SerializationUtils.bytesToObject(entry.getValue(), getObjectClass()));
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not implemented");
            }

            @Override
            public void closeInt() {
                //Closing the iterator here results in 'pthread lock: Invalid argument' and termination of the JVM.
                //Is there a big memory leak because we don't close the iterator now?
//                try {
//                    iterator.close();
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                }
            }
        };
    }

    @Override
    public void dropAllData() {
        dataLock.lockWriteAll();
        Options options = new Options();
        try {
            db.close();
            factory.destroy(databaseDir, options);
            options = new Options();
            options.createIfMissing(true);
            db = factory.open(databaseDir, options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        dataLock.unlockWriteAll();
    }

    @Override
    public void flush() {
        //Always flushed
    }

    @Override
    public void close() {
        synchronized (this) {
            if (db != null) {
                //Not yet closed
                try {
                    db.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                db = null;
            }
        }
    }

    @Override
    public long apprSize() {
        //TODO: find faster method. Tried already using db.getApproximateSizes() but gave bad results...
        return exactSize();
    }

    @Override
    public void writeInt0(Iterator<KeyValue<T>> entries) {
        long start = System.currentTimeMillis();
        WriteBatch writeBatch = db.createWriteBatch();
        while (entries.hasNext()) {
            KeyValue<T> entry = entries.next();
            long key = entry.getKey();
            byte[] keyInBytes = SerializationUtils.longToBytes(key);
            dataLock.lockWrite(key);
            byte[] currentValueInBytes = db.get(keyInBytes);
            T valueToWrite;
            if (entry.getValue() == null) {
                valueToWrite = null; //Delete value
            } else if (currentValueInBytes == null) {
                valueToWrite = entry.getValue();
            } else {
                valueToWrite = getCombinator().combine(SerializationUtils.bytesToObject(currentValueInBytes, getObjectClass()), entry.getValue());
            }
            if (valueToWrite == null) {
                writeBatch.delete(keyInBytes);
            } else {
                writeBatch.put(keyInBytes, SerializationUtils.objectToBytes(valueToWrite));
            }
            dataLock.unlockWrite(key);
        }
        db.write(writeBatch);
        try {
            writeBatch.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        totalTimeWrite += System.currentTimeMillis() - start;
    }
}
