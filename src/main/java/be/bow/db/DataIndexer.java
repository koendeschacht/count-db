package be.bow.db;

import java.util.List;

public interface DataIndexer<T> {

    List<Long> convertToIndexes(T object);

}
