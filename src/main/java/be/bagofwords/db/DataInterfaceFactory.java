package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.List;

public interface DataInterfaceFactory {

    <T> DataInterfaceConfig<T> dataInterface(String nameOfSubset, Class<T> objectClass);

    DataInterface<Long> createCountDataInterface(String subset);

    DataInterface<Long> createTmpCountDataInterface(String subset);

    DataInterface<Long> createInMemoryCountDataInterface(String name);

    <T extends Object> BaseDataInterface<T> createDataInterface(String subset, Class<T> objectClass, Combinator<T> combinator);

    List<DataInterfaceReference> getAllInterfaces();

    void terminate();

    void closeAllInterfaces();

    class DataInterfaceReference extends WeakReference<DataInterface> {

        private String subsetName;

        public DataInterfaceReference(BaseDataInterface referent, ReferenceQueue<DataInterface> referenceQueue) {
            super(referent, referenceQueue);
            this.subsetName = referent.getName();
        }

        public String getSubsetName() {
            return subsetName;
        }
    }

}
