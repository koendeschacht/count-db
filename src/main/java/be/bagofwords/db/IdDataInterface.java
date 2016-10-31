package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.SerializationUtils;

import java.util.Iterator;

/**
 * Created by koen on 31.10.16.
 */
public class IdDataInterface<S, T extends IdObject<S>> extends BaseDataInterface<T> {

    private final DataInterface<IdObjectList<S, T>> baseDataInterface;

    public IdDataInterface(Class<T> objectClass, Combinator<T> combinator, DataInterface<IdObjectList<S, T>> baseInterface) {
        super(objectClass, baseInterface.getName(), baseInterface.isTemporaryDataInterface(), combinator);
        this.baseDataInterface = baseInterface;
    }

    public T read(S key) {
        long longKey = convertToLong(key);
        IdObjectList<S, T> result = baseDataInterface.read(longKey);
        if (result != null) {
            for (T object : result) {
                if (object.getId().equals(key)) {
                    return object;
                }
            }
        }
        return null;
    }

    public void write(T object) {
        long longKey = convertToLong(object.getId());
        baseDataInterface.write(longKey, new IdObjectList<>(object));
    }

    private long convertToLong(S key) {
        if (key instanceof Long) {
            return (Long) key;
        } else if (key instanceof Integer) {
            return ((Integer) key).longValue();
        } else if (key instanceof CharSequence) {
            return HashUtils.hashCode((CharSequence) key);
        } else {
            return HashUtils.hashCode(SerializationUtils.serializeObject(key));
        }
    }

    @Override
    public void dropAllData() {
        baseDataInterface.dropAllData();
    }

    @Override
    public void flush() {
        baseDataInterface.flush();
    }

    @Override
    public DataInterface getCoreDataInterface() {
        return baseDataInterface.getCoreDataInterface();
    }

    public CloseableIterator<T> iterator() {
        CloseableIterator<IdObjectList<S, T>> baseIterator = baseDataInterface.valueIterator();
        return new CloseableIterator<T>() {
            private T next;
            private Iterator<T> currIt;

            {
                findNext();
            }

            @Override
            protected void closeInt() {
                baseIterator.close();
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public T next() {
                T curr = next;
                findNext();
                return curr;
            }

            private void findNext() {
                next = null;
                if (currIt == null || !currIt.hasNext()) {
                    currIt = null;
                    if (baseIterator.hasNext()) {
                        currIt = baseIterator.next().iterator();
                    }
                }
                if (currIt != null) {
                    next = currIt.next();
                }
            }
        };
    }


    @Override
    protected void doClose() {
        baseDataInterface.doClose();
    }
}
