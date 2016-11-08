package be.bagofwords.db;

import be.bagofwords.application.ApplicationContext;
import be.bagofwords.application.BaseApplicationContextFactory;
import be.bagofwords.application.MinimalApplicationContextFactory;
import be.bagofwords.application.SocketServer;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.helper.DataInterfaceFactoryFactory;
import be.bagofwords.db.remote.RemoteDataInterfaceServer;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/11/14.
 */
public class BaseTestDataInterface {

    @Parameterized.Parameters(name = "{0} {1}")
    public static List<Object[]> getManagers() throws IOException {
        List<Object[]> result = new ArrayList<>();

        List<DatabaseBackendType> backendTypes = new ArrayList<>();
        backendTypes.add(DatabaseBackendType.LEVELDB);
        backendTypes.add(DatabaseBackendType.MEMORY);
        backendTypes.add(DatabaseBackendType.REMOTE);
        backendTypes.add(DatabaseBackendType.FILE);
//        backendTypes.add(DatabaseBackendType.LMDB); --> too slow
        backendTypes.add(DatabaseBackendType.KYOTO);
        backendTypes.add(DatabaseBackendType.ROCKSDB);

        for (DatabaseBackendType backendType : backendTypes) {
            result.add(new Object[]{DatabaseCachingType.CACHED_AND_BLOOM, backendType});
            result.add(new Object[]{DatabaseCachingType.CACHED, backendType});
            result.add(new Object[]{DatabaseCachingType.DIRECT, backendType});
        }
        return result;
    }

    private DatabaseBackendType backendType;
    protected DatabaseCachingType type;
    protected DataInterfaceFactory dataInterfaceFactory;

    private RemoteDataInterfaceServer remoteDataInterfaceServer; //only created for remote backend
    private FileDataInterfaceFactory dataInterfaceServerFactory; //only created for remote backend
    private ApplicationContext context;

    public BaseTestDataInterface(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        this.backendType = backendType;
        this.type = type;
    }

    @Before
    public void setUp() throws Exception {
        HashMap<String, String> config = new HashMap<>();
        config.put("data_directory", "/tmp/dbServer_" + System.currentTimeMillis());
        config.put("remote_interface_host", "localhost");
        config.put("remote_interface_port", "1208");
        BaseApplicationContextFactory factory;
        if (backendType == DatabaseBackendType.REMOTE) {
            factory = new RemoteDatabaseApplicationContextFactor();
        } else {
            factory = new MinimalApplicationContextFactory();
        }
        context = factory.createApplicationContext(config);
        DataInterfaceFactoryFactory dataInterfaceFactoryFactory = new DataInterfaceFactoryFactory(context);
        dataInterfaceFactory = dataInterfaceFactoryFactory.createFactory(backendType);
        if (backendType == DatabaseBackendType.REMOTE) {
            dataInterfaceServerFactory = new FileDataInterfaceFactory(context);
            context.registerBean(dataInterfaceServerFactory);
            remoteDataInterfaceServer = new RemoteDataInterfaceServer(context);
        }
    }

    @After
    public void closeFactory() {
        context.terminateApplication();
        context.waitUntilTerminated();
    }

    public void createDataInterfaceFactory(DataInterfaceFactoryFactory dataInterfaceFactoryFactory) {
        this.dataInterfaceFactory = dataInterfaceFactoryFactory.createFactory(backendType);
    }

    protected DataInterface<Long> createCountDataInterface(String subsetName) {
        return dataInterfaceFactory.createDataInterface(type, subsetName + "_" + System.currentTimeMillis(), Long.class, new LongCombinator());
    }

    private static class RemoteDatabaseApplicationContextFactor extends MinimalApplicationContextFactory {
        @Override
        public void wireApplicationContext(ApplicationContext context) {
            super.wireApplicationContext(context);
            SocketServer socketServer = new SocketServer(1208);
            context.registerBean(socketServer);
            socketServer.start();
        }
    }

}
