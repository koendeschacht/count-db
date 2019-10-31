package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.experimental.id.IdDataInterface;
import be.bagofwords.db.experimental.id.IdObject;
import be.bagofwords.db.experimental.index.MultiDataIndexer;
import be.bagofwords.db.experimental.index.MultiDataInterfaceIndex;
import be.bagofwords.db.experimental.index.UniqueDataIndexer;
import be.bagofwords.db.experimental.index.UniqueDataInterfaceIndex;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.methods.JsonObjectSerializer;
import be.bagofwords.db.methods.ObjectSerializer;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;

public interface DataInterfaceFactory {

    <T> DataInterfaceConfig<T> dataInterface(String name, Class<T> objectClass, Class... genericParams);

    <T> MultiDataInterfaceIndex<T> multiIndex(DataInterface<T> dataInterface, String nameOfIndex, MultiDataIndexer<T> indexer);

    <T> UniqueDataInterfaceIndex<T> uniqueIndex(DataInterface<T> dataInterface, String nameOfIndex, UniqueDataIndexer<T> indexer);

    DataInterface<Long> createCountDataInterface(String name);

    DataInterface<Long> createTmpCountDataInterface(String name);

    DataInterface<Long> createInMemoryCountDataInterface(String name);

    default <T extends IdObject> IdDataInterfaceConfig<T> idDataInterface(String name, Class<T> objectClass) {
        return new IdDataInterfaceConfig<T>(dataInterface(name, List.class, objectClass));
    }

    default <T> DataInterface<T> createDataInterface(String name, Class<T> objectClass) {
        return createDataInterface(name, objectClass, new OverWriteCombinator<>(), new JsonObjectSerializer<>(objectClass));
    }

    default <T> DataInterface<T> createDataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        return createDataInterface(name, objectClass, combinator, new JsonObjectSerializer<>(objectClass));
    }

    default <T extends IdObject> IdDataInterface<T> createIdDataInterface(String name, Class<T> objectClass) {
        return idDataInterface(name, objectClass).create();
    }

    default <T extends IdObject> IdDataInterface<T> createIdDataInterface(String name, Class<T> objectClass, Combinator<T> combinator) {
        return idDataInterface(name, objectClass)
                .combinator(combinator)
                .create();
    }

    <T> DataInterface<T> createDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer);

    List<DataInterfaceReference> getAllInterfaces();

    void terminate();

    void closeAllInterfaces();

    class DataInterfaceReference extends WeakReference<DataInterface> {

        private String name;

        public DataInterfaceReference(BaseDataInterface referent, ReferenceQueue<DataInterface> referenceQueue) {
            super(referent, referenceQueue);
            this.name = referent.getName();
        }

        public String getName() {
            return name;
        }
    }

}
