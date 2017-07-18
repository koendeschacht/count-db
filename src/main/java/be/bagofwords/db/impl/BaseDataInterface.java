package be.bagofwords.db.impl;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.StringUtils;

public abstract class BaseDataInterface<T extends Object> implements DataInterface<T> {

    protected final Combinator<T> combinator;
    protected final Class<T> objectClass;
    protected final String name;
    protected final boolean isTemporaryDataInterface;
    protected final ObjectSerializer<T> objectSerializer;
    private final Object closeLock = new Object();
    private boolean wasClosed;

    public BaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporaryDataInterface) {
        if (StringUtils.isEmpty(name)) {
            throw new IllegalArgumentException("Name can not be null or empty");
        }
        this.objectClass = objectClass;
        this.name = name;
        this.isTemporaryDataInterface = isTemporaryDataInterface;
        this.combinator = combinator;
        this.objectSerializer = objectSerializer;
    }

    public abstract DataInterface<T> getCoreDataInterface();

    protected abstract void doClose();

    public Combinator<T> getCombinator() {
        return combinator;
    }

    @Override
    public ObjectSerializer<T> getObjectSerializer() {
        return objectSerializer;
    }


    public Class<T> getObjectClass() {
        return objectClass;
    }

    public String getName() {
        return name;
    }

    public final void close() {
        ifNotClosed(() -> {
                    if (isTemporaryDataInterface) {
                        dropAllData();
                    }
                    flush();
                    doClose();
                    wasClosed = true;
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

}
