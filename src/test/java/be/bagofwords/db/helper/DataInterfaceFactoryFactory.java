package be.bagofwords.db.helper;

import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseBackendType;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.memory.InMemoryDataInterfaceFactory;
import be.bagofwords.db.remote.RemoteDatabaseInterfaceFactory;
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
            case FILE:
                return new FileDataInterfaceFactory(context);
            case REMOTE:
                return new RemoteDatabaseInterfaceFactory(context);
            case MEMORY:
                return new InMemoryDataInterfaceFactory(context);
            default:
                throw new RuntimeException("Unknown backend type " + backendType);
        }
    }

}
