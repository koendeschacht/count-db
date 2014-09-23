package be.bagofwords.db.remote;

import be.bagofwords.application.file.OpenFilesManager;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class DatabaseServerDataInterfaceFactory extends FileDataInterfaceFactory {

    @Autowired
    public DatabaseServerDataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, FileCountDBEnvironmentProperties fileCountDBEnvironmentProperties) {
        super(openFilesManager, cachesManager, memoryManager, fileCountDBEnvironmentProperties);
    }

    public DatabaseServerDataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(openFilesManager, cachesManager, memoryManager, directory);
    }

    @Override
    protected <T> DataInterface<T> bloom(DataInterface<T> dataInterface) {
        return dataInterface; //We don't use a bloom filter on the server side, these should be present on the client side
    }
}
