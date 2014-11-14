package be.bagofwords.db.filedb;

import be.bagofwords.application.BowTaskScheduler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.db.combinator.Combinator;
import org.springframework.beans.factory.annotation.Autowired;

public class FileDataInterfaceFactory extends DataInterfaceFactory {

    private final MemoryManager memoryManager;
    private final String directory;

    @Autowired
    public FileDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, BowTaskScheduler taskScheduler, FileCountDBEnvironmentProperties fileCountDBEnvironmentProperties) {
        this(cachesManager, memoryManager, taskScheduler, fileCountDBEnvironmentProperties.getDataDirectory() + "server/");
    }

    public FileDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, BowTaskScheduler taskScheduler, String directory) {
        super(cachesManager, memoryManager, taskScheduler);
        this.memoryManager = memoryManager;
        this.directory = directory;
    }

    @Override
    public <T extends Object> DataInterface<T> createBaseDataInterface(final String nameOfSubset, final Class<T> objectClass, final Combinator<T> combinator, boolean isTemporaryDataInterface) {
        FileDataInterface<T> result = new FileDataInterface<>(memoryManager, combinator, objectClass, directory, nameOfSubset, isTemporaryDataInterface, taskScheduler);
        memoryManager.registerMemoryGobbler(result);
        return result;
    }

}
