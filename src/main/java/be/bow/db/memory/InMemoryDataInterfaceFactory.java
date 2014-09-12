package be.bow.db.memory;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.combinator.Combinator;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;

public class InMemoryDataInterfaceFactory extends DataInterfaceFactory {

    public InMemoryDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager) {
        super(cachesManager, memoryManager);
    }

    @Override
    protected <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        return new InMemoryDataInterface<>(nameOfSubset, objectClass, combinator);
    }
}
