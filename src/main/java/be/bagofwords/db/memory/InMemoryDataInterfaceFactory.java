package be.bagofwords.db.memory;

import be.bagofwords.application.ApplicationContext;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;

public class InMemoryDataInterfaceFactory extends DataInterfaceFactory {

    public InMemoryDataInterfaceFactory(ApplicationContext context) {
        super(context);
    }

    @Override
    public <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new InMemoryDataInterface<>(nameOfSubset, objectClass, combinator);
    }
}
