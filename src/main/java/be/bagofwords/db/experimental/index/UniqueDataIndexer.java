package be.bagofwords.db.experimental.index;

public interface UniqueDataIndexer<T> {

    Long convertToIndex(T object);

}
