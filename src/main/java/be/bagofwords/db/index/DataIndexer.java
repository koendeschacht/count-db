package be.bagofwords.db.index;

import java.util.List;

public interface DataIndexer<T> {

    List<Long> convertToIndexes(T object);

}
