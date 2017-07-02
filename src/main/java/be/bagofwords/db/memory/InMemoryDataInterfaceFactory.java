package be.bagofwords.db.memory;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.minidepi.ApplicationContext;

public class InMemoryDataInterfaceFactory extends BaseDataInterfaceFactory {

    public InMemoryDataInterfaceFactory(ApplicationContext context) {
        super(context);
    }

    @Override
    protected <T extends Object> BaseDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporaryDataInterface) {
        return new InMemoryDataInterface<>(name, objectClass, combinator);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return InMemoryDataInterface.class;
    }
}
