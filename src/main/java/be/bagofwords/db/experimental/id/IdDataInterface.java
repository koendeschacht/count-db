package be.bagofwords.db.experimental.id;

import be.bagofwords.db.DataInterface;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.DataIterable;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.StreamUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

public class IdDataInterface<T extends IdObject> implements DataIterable<T> {

    private DataInterface<List> dataInterface;

    public IdDataInterface(DataInterface<List> baseInterface) {
        this.dataInterface = baseInterface;
    }

    public T read(String id) {
        List values = dataInterface.read(id);
        if (values == null) {
            return null;
        }
        for (Object object : values) {
            T value = (T) object;
            if (Objects.equals(id, value.getId())) {
                return value;
            }
        }
        return null;
    }

    public void write(T object) {
        dataInterface.write(object.getId(), singletonList(object));
    }

    public void write(Iterator<T> objects) {
        write(IterableUtils.iterator(objects));
    }

    public void write(CloseableIterator<T> objects) {
        dataInterface.write(IterableUtils.mapIterator(objects, object -> new KeyValue<>(HashUtils.hashCode(object.getId()), singletonList(object))));
    }

    public Stream<T> stream() {
        return StreamUtils.stream(this, true);
    }

    public Stream<T> stream(Predicate<T> valueFilter) {
        return StreamUtils.stream(iterator(valueFilter), apprSize(), true);
    }

    @Override
    public CloseableIterator<T> iterator() {
        CloseableIterator<KeyValue<List>> baseIterator = dataInterface.iterator();

        return new CloseableIterator<T>() {

            private Iterator<T> listIt = Collections.emptyIterator();

            @Override
            protected void closeInt() {
                baseIterator.close();
            }

            @Override
            public boolean hasNext() {
                return listIt.hasNext() || baseIterator.hasNext();
            }

            @Override
            public T next() {
                if (!listIt.hasNext()) {
                    listIt = baseIterator.next().getValue().iterator();
                }
                return listIt.next();
            }
        };
    }

    public CloseableIterator<T> iterator(Predicate<T> valueFilter) {
        final CloseableIterator<T> keyValueIterator = iterator();
        return IterableUtils.iterator(new SimpleIterator<T>() {
            @Override
            public T next() {
                while (keyValueIterator.hasNext()) {
                    T next = keyValueIterator.next();
                    if (valueFilter.test(next)) {
                        return next;
                    }
                }
                return null;
            }

            @Override
            public void close() {
                keyValueIterator.close();
            }
        });
    }

    @Override
    public long apprSize() {
        return dataInterface.apprSize();
    }

    public void flush() {
        dataInterface.flush();
    }

    public void dropAllData() {
        dataInterface.dropAllData();
    }

    public void close() {
        dataInterface.close();
    }
}
