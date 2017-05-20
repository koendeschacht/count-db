package be.bagofwords.db;

import be.bagofwords.MetaDataProperties;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.MetaDataStore;

import static be.bagofwords.db.impl.BaseDataInterfaceFactory.META_DATA_STORAGE;

public abstract class CoreDataInterface<T> extends BaseDataInterface<T> {

    private MetaDataStore metaDataStore;
    private long flushInd;

    public CoreDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporary) {
        super(name, objectClass, combinator, isTemporary);
    }

    public void setMetaDataStore(MetaDataStore metaDataStore) {
        this.metaDataStore = metaDataStore;
        this.flushInd = readFlushInd();
    }

    @Override
    public DataInterface<T> getCoreDataInterface() {
        return this;
    }

    @Override
    public void flush() {
        flushInd++;
        metaDataStore.write(this, MetaDataProperties.LAST_FLUSH, flushInd);
        flushImpl();
    }

    protected abstract void flushImpl();

    @Override
    public long lastFlush() {
        return flushInd;
    }

    private long readFlushInd() {
        String lastFlushAsString = metaDataStore.getString(this, MetaDataProperties.LAST_FLUSH);
        if (lastFlushAsString == null) {
            return 0;
        } else {
            return Long.parseLong(lastFlushAsString);
        }
    }
}
