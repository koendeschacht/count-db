package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.impl.MetaDataStore;
import be.bagofwords.logging.Log;

import java.util.stream.Stream;

/**
 * Created by koen on 16/07/17.
 */
public abstract class BaseDataInterfaceIndex<T> {

    private final Object buildIndexLock = new Object();
    protected final DataInterface<T> dataInterface;
    protected final MetaDataStore metaDataStore;
    protected long lastSync = 0;

    protected BaseDataInterfaceIndex(DataInterface<T> dataInterface, MetaDataStore metaDataStore) {
        this.dataInterface = dataInterface;
        this.metaDataStore = metaDataStore;
    }

    protected void ensureIndexUpToDate() {
        synchronized (buildIndexLock) {
            if (dataInterface.lastFlush() != lastSync) {
                Log.i("Index out of date, rebuilding index " + getIndexName());
                long start = System.currentTimeMillis();
                rebuildIndex();
                Log.i("Rebuilding index " + getIndexName() + " took " + (System.currentTimeMillis() - start) + " ms");
            }
        }
    }

    protected Stream<T> streamValuesForKeys(Stream<Long> keyStream) {
        return dataInterface.streamValues(keyStream);
    }

    protected abstract String getIndexName();

    protected abstract void rebuildIndex();
}
