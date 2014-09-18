package be.bow.db.helper;

import be.bow.application.annotations.BowComponent;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseBackendType;
import be.bow.db.filedb.FileDataInterfaceFactory;
import be.bow.db.kyoto.KyotoDataInterfaceFactory;
import be.bow.db.leveldb.LevelDBDataInterfaceFactory;
import be.bow.db.lmdb.LMDBDataInterfaceFactory;
import be.bow.db.memory.InMemoryDataInterfaceFactory;
import be.bow.db.remote.RemoteDatabaseInterfaceFactory;
import be.bow.db.rocksdb.RocksDBDataInterfaceFactory;
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
    private UnitTestEnvironmentProperties environmentProperties;

    public DataInterfaceFactory createFactory(DatabaseBackendType backendType) {
        switch (backendType) {
            case FILE:
                return new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, environmentProperties.getDataDirectory() + "server/");
            case REMOTE:
                return new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDatabaseServerAddress(), environmentProperties.getDataInterfaceServerPort());
            case MEMORY:
                return new InMemoryDataInterfaceFactory(cachesManager, memoryManager);
            case LEVELDB:
                return new LevelDBDataInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDataDirectory() + "leveLDB/");
            case LMDB:
                return new LMDBDataInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDataDirectory() + "lmDB/");
            case KYOTO:
                return new KyotoDataInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDataDirectory() + "kyotoDB/");
            case ROCKSDB:
                return new RocksDBDataInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDataDirectory() + "rocksDB/", false);
            case ROCKSDB_PATCHED:
                return new RocksDBDataInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDataDirectory() + "rocksDB/", true);
            default:
                throw new RuntimeException("Unknown backend type " + backendType);
        }
    }

}
