package be.bagofwords.db.memory;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;

public class InMemoryDataInterfaceFactory extends DataInterfaceFactory {

    public InMemoryDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager) {
        super(cachesManager, memoryManager);
    }

    @Override
    public <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        return new InMemoryDataInterface<>(nameOfSubset, objectClass, combinator);
    }
}
