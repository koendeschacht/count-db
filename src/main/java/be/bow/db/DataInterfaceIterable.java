package be.bow.db;

import be.bow.iterator.CloseableIterator;
import be.bow.iterator.DataIterable;
import be.bow.util.KeyValue;

public class DataInterfaceIterable<T extends Object> implements DataIterable<KeyValue<T>> {

    private final DataInterface<T> dataInterface;

    public DataInterfaceIterable(DataInterface<T> dataInterface) {
        this.dataInterface = dataInterface;
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        return dataInterface.iterator();
    }

    @Override
    public long apprSize() {
        return dataInterface.apprSize();
    }
}
