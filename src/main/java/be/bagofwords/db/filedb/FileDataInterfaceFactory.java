package be.bagofwords.db.filedb;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.minidepi.ApplicationContext;

public class FileDataInterfaceFactory extends DataInterfaceFactory {

    private final MemoryManager memoryManager;
    private final String directory;

    public FileDataInterfaceFactory(ApplicationContext context) {
        super(context);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.directory = context.getProperty("data_directory");
    }

    @Override
    public <T extends Object> DataInterface<T> createBaseDataInterface(final String nameOfSubset, final Class<T> objectClass, final Combinator<T> combinator, boolean isTemporaryDataInterface) {
        FileDataInterface<T> result = new FileDataInterface<>(memoryManager, combinator, objectClass, directory, nameOfSubset, isTemporaryDataInterface, taskScheduler);
        memoryManager.registerMemoryGobbler(result);
        return result;
    }

}
