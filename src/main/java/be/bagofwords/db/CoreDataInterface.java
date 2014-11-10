package be.bagofwords.db;

import be.bagofwords.db.combinator.Combinator;

public abstract class CoreDataInterface<T> extends DataInterface<T> {

    public CoreDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporary) {
        super(name, objectClass, combinator, isTemporary);
    }

    @Override
    public DataInterface getCoreDataInterface() {
        return this;
    }

}
