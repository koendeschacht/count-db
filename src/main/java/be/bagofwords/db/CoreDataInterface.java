package be.bagofwords.db;

import be.bagofwords.MetaDataProperties;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.MetaDataStore;

public abstract class CoreDataInterface<T> extends BaseDataInterface<T> {

    private final MetaDataStore metaDataStore;

    public CoreDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporary, MetaDataStore metaDataStore) {
        super(name, objectClass, combinator, isTemporary);
        this.metaDataStore = metaDataStore;
    }

    @Override
    public DataInterface getCoreDataInterface() {
        return this;
    }

    @Override
    public long lastWrite() {
        String lastWriteAsString = metaDataStore.getString(this, MetaDataProperties.LAST_WRITE);
        if (lastWriteAsString == null) {
            return 0;
        } else {
            return Long.parseLong(lastWriteAsString);
        }
    }
}
