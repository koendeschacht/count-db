package be.bow.db.filedb4;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.Combinator;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;

public class FileDataInterfaceFactory extends DataInterfaceFactory {

    private MemoryManager memoryManager;
    private OpenFilesManager openFilesManager;
    private final String directory;

    public FileDataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(cachesManager, memoryManager);
        this.memoryManager = memoryManager;
        this.openFilesManager = openFilesManager;
        this.directory = directory;
    }

    @Override
    protected <T extends Object> DataInterface<T> createBaseDataInterface(final String nameOfSubset, final Class<T> objectClass, final Combinator<T> combinator) {
        FileDataInterface<T> result = new FileDataInterface<>(openFilesManager, combinator, objectClass, directory, nameOfSubset);
        openFilesManager.registerFilesCollection(result);
        memoryManager.registerMemoryGobbler(result);
        return result;
    }

}
