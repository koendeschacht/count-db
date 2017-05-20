package be.bagofwords.db.memory;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.minidepi.ApplicationContext;

public class InMemoryDataInterfaceFactory extends BaseDataInterfaceFactory {

    public InMemoryDataInterfaceFactory(ApplicationContext context) {
        super(context);
    }

    @Override
    protected <T extends Object> BaseDataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new InMemoryDataInterface<>(nameOfSubset, objectClass, combinator);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return InMemoryDataInterface.class;
    }
}
