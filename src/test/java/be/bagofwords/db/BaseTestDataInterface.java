package be.bagofwords.db;

import be.bagofwords.application.MinimalApplicationDependencies;
import be.bagofwords.db.combinator.OverWriteCombinator;
import be.bagofwords.db.methods.LongObjectSerializer;
import be.bagofwords.web.SocketServer;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.helper.DataInterfaceFactoryFactory;
import be.bagofwords.db.remote.RemoteDataInterfaceServer;
import be.bagofwords.minidepi.ApplicationContext;
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

    private static long count = System.currentTimeMillis();

    @Parameterized.Parameters(name = "{0} {1}")
    public static List<Object[]> getManagers() throws IOException {
        List<Object[]> result = new ArrayList<>();

        List<DatabaseBackendType> backendTypes = new ArrayList<>();
        backendTypes.add(DatabaseBackendType.MEMORY);
        backendTypes.add(DatabaseBackendType.REMOTE);
        backendTypes.add(DatabaseBackendType.FILE);

        for (DatabaseBackendType backendType : backendTypes) {
            result.add(new Object[]{DatabaseCachingType.CACHED_AND_BLOOM, backendType});
            result.add(new Object[]{DatabaseCachingType.CACHED, backendType});
            result.add(new Object[]{DatabaseCachingType.DIRECT, backendType});
        }
        return result;
    }

    protected DatabaseBackendType backendType;
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
        config.put("socket.host", "localhost");
        config.put("socket.port", "1208");
        context = new ApplicationContext(config);
        context.registerBean(MinimalApplicationDependencies.class);
        DataInterfaceFactoryFactory dataInterfaceFactoryFactory = new DataInterfaceFactoryFactory(context);
        if (backendType == DatabaseBackendType.REMOTE) {
            dataInterfaceServerFactory = context.getBean(FileDataInterfaceFactory.class);
            remoteDataInterfaceServer = context.getBean(RemoteDataInterfaceServer.class);
            context.registerBean(SocketServer.class);
        }
        dataInterfaceFactory = dataInterfaceFactoryFactory.createFactory(backendType);
    }

    @After
    public void closeFactory() {
        if (dataInterfaceFactory != null) {
            dataInterfaceFactory.closeAllInterfaces();
        }
        context.terminate();
    }

    public void createDataInterfaceFactory(DataInterfaceFactoryFactory dataInterfaceFactoryFactory) {
        this.dataInterfaceFactory = dataInterfaceFactoryFactory.createFactory(backendType);
    }

    protected DataInterface<Long> createCountDataInterface(String name) {
        return dataInterfaceFactory.dataInterface(name + "_" + (count++), Long.class).combinator(new LongCombinator()).serializer(new LongObjectSerializer()).caching(type).create();
    }

    protected <T> DataInterfaceConfig<T> createDataInterface(String name, Class<T> _class) {
        return dataInterfaceFactory.dataInterface(name + "_" + (count++), _class);
    }

}
