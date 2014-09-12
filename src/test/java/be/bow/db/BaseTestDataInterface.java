package be.bow.db;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.remote.RemoteDataInterfaceServer;
import be.bow.db.remote.DatabaseServerDataInterfaceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.TestContextManager;

import java.io.IOException;
import java.util.ArrayList;
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

        for (DatabaseBackendType backendType : backendTypes) {
            result.add(new Object[]{DatabaseCachingType.CACHED_AND_BLOOM, backendType});
            result.add(new Object[]{DatabaseCachingType.CACHED, backendType});
            result.add(new Object[]{DatabaseCachingType.DIRECT, backendType});
        }
        return result;
    }

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private MemoryManager memoryManager;
    @Autowired
    private OpenFilesManager openFilesManager;

    private DatabaseBackendType backendType;
    protected DatabaseCachingType type;
    protected DataInterfaceFactory dataInterfaceFactory;

    private RemoteDataInterfaceServer remoteDataInterfaceServer; //only created for remote backend
    private DatabaseServerDataInterfaceFactory dataInterfaceServerFactory; //only created for remote backend

    public BaseTestDataInterface(DatabaseCachingType type, DatabaseBackendType backendType) throws Exception {
        this.backendType = backendType;
        this.type = type;
    }


    @Before
    public void setUp() throws Exception {
        TestContextManager testContextManager = new TestContextManager(getClass());
        testContextManager.prepareTestInstance(this);

        if (backendType == DatabaseBackendType.REMOTE) {
            dataInterfaceServerFactory = new DatabaseServerDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, "/tmp/dbServer");
            remoteDataInterfaceServer = new RemoteDataInterfaceServer(dataInterfaceServerFactory);
            remoteDataInterfaceServer.start();
        }
    }

    @After
    public void closeFactory() {
        dataInterfaceFactory.close();
        if (remoteDataInterfaceServer != null) {
            remoteDataInterfaceServer.close();
            remoteDataInterfaceServer.waitForFinish();
            dataInterfaceServerFactory.close();
        }
    }

    @Autowired
    public void createDataInterfaceFactory(DataInterfaceFactoryFactory dataInterfaceFactoryFactory) {
        this.dataInterfaceFactory = dataInterfaceFactoryFactory.createFactory(backendType);
    }

    protected DataInterface<Long> createCountDataInterface(String subsetName) {
        return dataInterfaceFactory.createDataInterface(type, subsetName, Long.class, new LongCombinator());
    }


}
