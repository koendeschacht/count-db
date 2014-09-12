package be.bow.db;

import be.bow.application.annotations.BowComponent;
import be.bow.application.environment.CountDBEnvironmentProperties;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.filedb4.FileDataInterfaceFactory;
import be.bow.db.leveldb.LevelDBDataInterfaceFactory;
import be.bow.db.memory.InMemoryDataInterfaceFactory;
import be.bow.db.remote.RemoteDatabaseInterfaceFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */

@BowComponent
public class DataInterfaceFactoryFactory {

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private MemoryManager memoryManager;
    @Autowired
    private OpenFilesManager openFilesManager;
    @Autowired
    private CountDBEnvironmentProperties environmentProperties;

    public DataInterfaceFactory createFactory(DatabaseBackendType backendType) {
        switch (backendType) {
            case FILE:
                return new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, environmentProperties.getDataDirectory() + "server/");
            case REMOTE:
                return new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDatabaseServerAddress(), 1208);
            case MEMORY:
                return new InMemoryDataInterfaceFactory(cachesManager, memoryManager);
            case LEVELDB:
                return new LevelDBDataInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDataDirectory() + "leveLDB/");
            default:
                throw new RuntimeException("Unknown backend type " + backendType);
        }
    }

}
