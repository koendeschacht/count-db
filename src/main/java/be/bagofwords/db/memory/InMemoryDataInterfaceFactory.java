package be.bagofwords.db.memory;

import be.bagofwords.application.BowTaskScheduler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;

public class InMemoryDataInterfaceFactory extends DataInterfaceFactory {

    public InMemoryDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, BowTaskScheduler taskScheduler) {
        super(cachesManager, memoryManager, taskScheduler);
    }

    @Override
    public <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new InMemoryDataInterface<>(nameOfSubset, objectClass, combinator, isTemporaryDataInterface);
    }
}
