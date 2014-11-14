package be.bagofwords.db.remote;

import be.bagofwords.application.BowTaskScheduler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.db.combinator.Combinator;
import org.springframework.beans.factory.annotation.Autowired;

public class RemoteDatabaseInterfaceFactory extends DataInterfaceFactory {

    private final String host;
    private final int port;

    @Autowired
    public RemoteDatabaseInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, BowTaskScheduler taskScheduler, RemoteCountDBEnvironmentProperties environmentProperties) {
        this(cachesManager, memoryManager, taskScheduler, environmentProperties.getDatabaseServerAddress(), environmentProperties.getDataInterfaceServerPort());
    }

    public RemoteDatabaseInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, BowTaskScheduler taskScheduler, String host, int port) {
        super(cachesManager, memoryManager, taskScheduler);
        this.host = host;
        this.port = port;
    }

    @Override
    public synchronized <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new RemoteDataInterface<>(nameOfSubset, objectClass, combinator, host, port, isTemporaryDataInterface, taskScheduler);
    }

}
