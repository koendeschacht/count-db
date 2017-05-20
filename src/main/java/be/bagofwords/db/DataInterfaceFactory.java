package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.experimental.index.DataIndexer;
import be.bagofwords.db.experimental.index.DataInterfaceIndex;
import be.bagofwords.db.impl.BaseDataInterface;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;

public interface DataInterfaceFactory {

    <T> DataInterfaceConfig<T> dataInterface(String name, Class<T> objectClass);

    <T> DataInterfaceIndex<T> index(DataInterface<T> dataInterface, String nameOfIndex, DataIndexer<T> indexer);

    DataInterface<Long> createCountDataInterface(String name);

    DataInterface<Long> createTmpCountDataInterface(String name);

    DataInterface<Long> createInMemoryCountDataInterface(String name);

    <T extends Object> BaseDataInterface<T> createDataInterface(String name, Class<T> objectClass, Combinator<T> combinator);

    List<DataInterfaceReference> getAllInterfaces();

    void terminate();

    void closeAllInterfaces();

    class DataInterfaceReference extends WeakReference<DataInterface> {

        private String name;

        public DataInterfaceReference(BaseDataInterface referent, ReferenceQueue<DataInterface> referenceQueue) {
            super(referent, referenceQueue);
            this.name = referent.getName();
        }

        public String getSubsetName() {
            return name;
        }
    }

}
