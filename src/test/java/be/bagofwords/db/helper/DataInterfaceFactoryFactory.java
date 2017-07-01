package be.bagofwords.db.helper;

import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.experimental.kyoto.KyotoDataInterfaceFactory;
import be.bagofwords.db.experimental.lmdb.LMDBDataInterfaceFactory;
import be.bagofwords.db.experimental.rocksdb.RocksDBDataInterfaceFactory;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.leveldb.LevelDBDataInterfaceFactory;
import be.bagofwords.db.memory.InMemoryDataInterfaceFactory;
import be.bagofwords.db.newfiledb.NewFileDataInterfaceFactory;
import be.bagofwords.db.remote.RemoteDatabaseInterfaceFactory;
import be.bagofwords.db.speedy.SpeedyDataInterfaceFactory;
import be.bagofwords.minidepi.ApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */

public class DataInterfaceFactoryFactory {

    private ApplicationContext context;

    public DataInterfaceFactoryFactory(ApplicationContext applicationContext) {
        this.context = applicationContext;
    }

    public DataInterfaceFactory createFactory(DatabaseBackendType backendType) {
        DataInterfaceFactory factory = createFactoryImpl(backendType);
        context.registerBean(factory);
        return factory;
    }

    private DataInterfaceFactory createFactoryImpl(DatabaseBackendType backendType) {
        switch (backendType) {
            case SPEED:
                return new SpeedyDataInterfaceFactory(context);
            case FILE:
                return new FileDataInterfaceFactory(context);
            case NEW_FILE:
                return new NewFileDataInterfaceFactory(context);
            case REMOTE:
                return new RemoteDatabaseInterfaceFactory(context);
            case MEMORY:
                return new InMemoryDataInterfaceFactory(context);
            case LEVELDB:
                return new LevelDBDataInterfaceFactory(context);
            case LMDB:
                return new LMDBDataInterfaceFactory(context);
            case KYOTO:
                return new KyotoDataInterfaceFactory(context);
            case ROCKSDB:
                return new RocksDBDataInterfaceFactory(context, false);
            case ROCKSDB_PATCHED:
                return new RocksDBDataInterfaceFactory(context, true);
            default:
                throw new RuntimeException("Unknown backend type " + backendType);
        }
    }

}
