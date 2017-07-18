package be.bagofwords.db.experimental.index;

import java.util.List;

public interface MultiDataIndexer<T> {

    List<Long> convertToIndexes(T object);

}
