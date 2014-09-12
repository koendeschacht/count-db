package be.bow.db.remote;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.filedb4.FileDataInterfaceFactory;

public class DatabaseServerDataInterfaceFactory extends FileDataInterfaceFactory {

    public DatabaseServerDataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(openFilesManager, cachesManager, memoryManager, directory);
    }

    @Override
    protected <T> DataInterface<T> bloom(DataInterface<T> dataInterface) {
        return dataInterface; //We don't use a bloom filter on the server side, these should be present on the client side
    }
}
