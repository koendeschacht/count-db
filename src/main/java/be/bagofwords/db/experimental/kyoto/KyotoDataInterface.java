package be.bagofwords.db.experimental.kyoto;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;
import kyotocabinet.Cursor;
import kyotocabinet.DB;

import java.io.File;
import java.util.*;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
public class KyotoDataInterface<T> extends CoreDataInterface<T> {

    private final static int BATCH_SIZE = 10000;
    private DB db;

    protected KyotoDataInterface(String name, String path, Class<T> objectClass, Combinator<T> combinator) {
        super(name, objectClass, combinator);
        db = new DB();
        File file = new File(new File(path), name + ".kcf");
        File parentDir = file.getParentFile();
        if (parentDir.isFile()) {
            throw new RuntimeException("Path " + parentDir.getAbsolutePath() + " is a file!");
        }
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        boolean success = db.open(file.getAbsolutePath(), DB.OWRITER | DB.OCREATE | DB.OREADER);
        if (!success) {
            throw new RuntimeException("Failed to open Kyoto DB at " + file.getAbsolutePath());
        }
    }

    @Override
    public T read(long key) {
        byte[] bytes = db.get(SerializationUtils.longToBytes(key));
        return SerializationUtils.bytesToObject(bytes, getObjectClass());
    }

    @Override
    public void writeInt0(long key, T value) {
        byte[] keyAsBytes = SerializationUtils.longToBytes(key);
        if (value == null) {
            db.remove(keyAsBytes);
        } else {
            if (getCombinator() instanceof LongCombinator) {
                //Performance improvement
                db.increment(keyAsBytes, (Long) value, 0l);
            } else {
                T valueToWrite;
                T currentValue = SerializationUtils.bytesToObject(db.get(keyAsBytes), getObjectClass());
                if (currentValue == null) {
                    valueToWrite = value;
                } else {
                    valueToWrite = getCombinator().combine(currentValue, value);
                }
                byte[] valueAsBytes = SerializationUtils.objectToBytes(valueToWrite, getObjectClass());
                db.set(keyAsBytes, valueAsBytes);
            }
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        final Cursor cursor = db.cursor();
        return new CloseableIterator<KeyValue<T>>() {

            private boolean hasNext = cursor.jump();

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public KeyValue<T> next() {
                byte[][] keyAndVal = cursor.get(false);
                hasNext = cursor.step();
                if (!hasNext) {
                    close();
                }
                return new KeyValue<>(SerializationUtils.bytesToLong(keyAndVal[0]), SerializationUtils.bytesToObject(keyAndVal[1], getObjectClass()));
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not impemented!");
            }

            @Override
            public void closeInt() {
                if (hasNext && db != null) {
                    cursor.disable();
                }
            }

        };
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        return new CloseableIterator<KeyValue<T>>() {

            private Iterator<KeyValue<T>> valueBatch;

            {
                findNextBatch();
            }

            private void findNextBatch() {
                List<Long> keys = new ArrayList<>();
                while (keyIterator.hasNext() && keys.size() < BATCH_SIZE) {
                    keys.add(keyIterator.next());
                }
                if (!keys.isEmpty()) {
                    byte[][] keysAsBytes = new byte[keys.size()][];
                    for (int i = 0; i < keys.size(); i++) {
                        keysAsBytes[i] = SerializationUtils.longToBytes(keys.get(i));
                    }
                    byte[][] keysAndValues = db.get_bulk(keysAsBytes, false);
                    Map<Long, T> parsedValues = new HashMap<>();
                    for (int i = 0; i < keysAndValues.length; i += 2) {
                        long key = SerializationUtils.bytesToLong(keysAndValues[i]);
                        T value = SerializationUtils.bytesToObject(keysAndValues[i + 1], getObjectClass());
                        parsedValues.put(key, value);
                    }
                    List<KeyValue<T>> allValues = new ArrayList<>();
                    for (Long key : keys) {
                        allValues.add(new KeyValue<>(key, parsedValues.get(key)));
                    }
                    valueBatch = allValues.iterator();
                } else {
                    valueBatch = null;
                }
            }

            @Override
            public boolean hasNext() {
                return valueBatch != null;
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> next = valueBatch.next();
                if (!valueBatch.hasNext()) {
                    findNextBatch();
                }
                return next;
            }

            @Override
            public void closeInt() {
                //all good
            }
        };
    }

    @Override
    public void optimizeForReading() {
        //do nothing?
    }

    @Override
    public void writeInt0(Iterator<KeyValue<T>> entries) {
        while (entries.hasNext()) {
            //Write values in batch:
            Map<Long, T> valuesToWrite = new HashMap<>();
            List<Long> valuesToDelete = new ArrayList<>();
            int numOfValues = 0;
            while (numOfValues < BATCH_SIZE && entries.hasNext()) {
                KeyValue<T> entry = entries.next();
                if (entry.getValue() == null) {
                    valuesToDelete.add(entry.getKey());
                } else {
                    valuesToWrite.put(entry.getKey(), entry.getValue());
                }
                numOfValues++;
            }
            bulkDelete(valuesToDelete);
            bulkWrite(valuesToWrite);
        }
    }

    private void bulkWrite(Map<Long, T> valuesToWrite) {
        //First read current values in bulk
        Map<Long, T> currentValues = new HashMap<>();
        CloseableIterator<KeyValue<T>> valueIt = iterator(valuesToWrite.keySet().iterator());
        while (valueIt.hasNext()) {
            KeyValue<T> curr = valueIt.next();
            if (curr.getValue() != null) {
                currentValues.put(curr.getKey(), curr.getValue());
            }
        }
        valueIt.close();
        byte[][] keysAndValues = new byte[valuesToWrite.size() * 2][];
        int ind = 0;
        for (Map.Entry<Long, T> value : valuesToWrite.entrySet()) {
            T currentValue = currentValues.get(value.getKey());
            T valueToWrite;
            if (currentValue == null) {
                valueToWrite = value.getValue();
            } else {
                valueToWrite = getCombinator().combine(currentValue, value.getValue());
            }
            keysAndValues[ind] = SerializationUtils.longToBytes(value.getKey());
            keysAndValues[ind + 1] = SerializationUtils.objectToBytes(valueToWrite, getObjectClass());
            ind += 2;
        }
        db.set_bulk(keysAndValues, false);
    }

    private void bulkDelete(List<Long> valuesToDelete) {
        if (valuesToDelete.size() > 0) {
            byte[][] allKeys = new byte[valuesToDelete.size()][];
            for (int i = 0; i < valuesToDelete.size(); i++) {
                allKeys[i] = SerializationUtils.longToBytes(valuesToDelete.get(i));
            }
            db.remove_bulk(allKeys, false);
        }
    }

    @Override
    public void dropAllData() {
        db.clear();
    }

    @Override
    public void flush() {
        //Always flushed
    }

    @Override
    protected void doClose() {
        synchronized (this) {
            db.close();
            db = null;
        }
    }

    @Override
    public DataInterface getImplementingDataInterface() {
        return null;
    }

    @Override
    public long apprSize() {
        return db.count();
    }
}