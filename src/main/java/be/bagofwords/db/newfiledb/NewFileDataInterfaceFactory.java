package be.bagofwords.db.newfiledb;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.logging.Log;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.minidepi.ApplicationContext;

public class NewFileDataInterfaceFactory extends BaseDataInterfaceFactory {

    private final MemoryManager memoryManager;
    private final String directory;

    public NewFileDataInterfaceFactory(ApplicationContext context) {
        super(context);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.directory = context.getProperty("data_directory");
    }

    @Override
    protected <T extends Object> BaseDataInterface<T> createBaseDataInterface(final String name, final Class<T> objectClass, final Combinator<T> combinator, boolean isTemporaryDataInterface) {
        Log.i("Creating file data interface " + name);
        NewFileDataInterface<T> result = new NewFileDataInterface<>(memoryManager, combinator, objectClass, directory, name, isTemporaryDataInterface, asyncJobService);
        memoryManager.registerMemoryGobbler(result);
        return result;
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return NewFileDataInterface.class;
    }

}
