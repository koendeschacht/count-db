package be.bagofwords.db.experimental.lmdb;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.DataLock;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;
import org.fusesource.lmdbjni.*;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/16/14.
 */

public class LMDBDataInterface<T> extends CoreDataInterface<T> {

    private final Env env;
    private final DataLock dataLock;
    private Database db;

    public LMDBDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, Env env) {
        super(name, objectClass, combinator);
        this.db = env.openDatabase(name);
        this.env = env;
        this.dataLock = new DataLock(false);
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        Transaction transaction = env.createTransaction();
        long numberOfValuesWritten = 0;
        while (entries.hasNext()) {
            KeyValue<T> next = entries.next();
            try {
                writeWithTransaction(next.getKey(), next.getValue(), transaction);
            } catch (Exception exp) {
                throw new RuntimeException("Failed to write multiple entries after " + numberOfValuesWritten + " values", exp);
            }
            numberOfValuesWritten++;
            if (numberOfValuesWritten > 1000) {
                transaction.commit();
                transaction = env.createTransaction();
//                UI.write("Wrote " + numberOfValuesWritten + " values");
                numberOfValuesWritten = 0;
            }
        }
        transaction.commit();
//        UI.write("Wrote " + numberOfValuesWritten + " values");
    }

    @Override
    public void write(long key, T value) {
        Transaction transaction = env.createTransaction();
        writeWithTransaction(key, value, transaction);
        transaction.commit();
    }

    private void writeWithTransaction(long key, T value, Transaction transaction) {
        dataLock.lockWrite(key);
        byte[] keysAsBytes = SerializationUtils.longToBytes(key);
        try {
            T currentValue = SerializationUtils.bytesToObject(db.get(transaction, keysAsBytes), getObjectClass());
            T combinedValue;
            if (currentValue == null || value == null) {
                combinedValue = value;
            } else {
                combinedValue = getCombinator().combine(currentValue, value);
            }
            if (combinedValue == null) {
                db.delete(transaction, keysAsBytes);
            } else {
                db.put(transaction, keysAsBytes, SerializationUtils.objectToBytes(combinedValue, getObjectClass()));
            }
        } catch (Exception exp) {
            throw new RuntimeException("Error while trying to write key " + key + "(=" + Arrays.toString(keysAsBytes) + ") with value " + value, exp);
        } finally {
            dataLock.unlockWrite(key);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        final Transaction transaction = env.createTransaction();
        final Cursor cursor = db.openCursor(transaction);
        return new CloseableIterator<KeyValue<T>>() {
            private Entry next;

            {
                next = cursor.get(GetOp.FIRST);
            }

            @Override
            protected void closeInt() {
                cursor.close();
                transaction.commit();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> result = new KeyValue<>(SerializationUtils.bytesToLong(next.getKey()), SerializationUtils.bytesToObject(next.getValue(), getObjectClass()));
                next = cursor.get(GetOp.NEXT);
                return result;
            }
        };
    }

    @Override
    public void optimizeForReading() {
        //do nothing?
    }

    @Override
    public T read(long key) {
        byte[] resultAsBytes = db.get(SerializationUtils.longToBytes(key));
        return SerializationUtils.bytesToObject(resultAsBytes, getObjectClass());
    }

    @Override
    public void dropAllData() {
        dataLock.lockWriteAll();
        db.drop(false);
        dataLock.unlockWriteAll();
    }

    @Override
    public void flush() {
        //always flushed
    }

    @Override
    public synchronized void doClose() {
        dataLock.lockWriteAll();
        db.close();
        dataLock.unlockWriteAll();
    }

    @Override
    public long apprSize() {
        //this does not seem to work? long approxSize= db.stat().ms_entries;
        return exactSize();
    }

}
