package be.bagofwords.db.memory;

import be.bagofwords.db.impl.DataInterfaceFactoryImpl;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.minidepi.ApplicationContext;

public class InMemoryDataInterfaceFactory extends DataInterfaceFactoryImpl {

    public InMemoryDataInterfaceFactory(ApplicationContext context) {
        super(context);
    }

    @Override
    protected <T extends Object> BaseDataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new InMemoryDataInterface<>(nameOfSubset, objectClass, combinator, getMetaDataStore());
    }
}
