package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.UpdateListener;
import be.bagofwords.db.impl.UpdateListenerCollection;
import be.bagofwords.db.methods.ObjectSerializer;

public abstract class CoreDataInterface<T> extends BaseDataInterface<T> {

    protected final UpdateListenerCollection<T> updateListenerCollection = new UpdateListenerCollection<>();

    public CoreDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporary) {
        super(name, objectClass, combinator, objectSerializer, isTemporary);
    }

    @Override
    public DataInterface<T> getCoreDataInterface() {
        return this;
    }

    @Override
    public void registerUpdateListener(UpdateListener<T> updateListener) {
        updateListenerCollection.registerUpdateListener(updateListener);
    }

    @Override
    public void flush() {
        flushImpl();
        updateListenerCollection.dataFlushed();
    }

    protected abstract void flushImpl();

}
