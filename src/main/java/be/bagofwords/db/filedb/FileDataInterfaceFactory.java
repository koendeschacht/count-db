package be.bagofwords.db.filedb;

import be.bagofwords.application.file.OpenFilesManager;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.util.SafeThread;
import be.bagofwords.util.Utils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;

public class FileDataInterfaceFactory extends DataInterfaceFactory {

    private final MemoryManager memoryManager;
    private final OpenFilesManager openFilesManager;
    private final String directory;
    private final List<FileDataInterface> interfaces;
    private final WriteCleanFilesListThread writeCleanFilesListThread;

    @Autowired
    public FileDataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, FileCountDBEnvironmentProperties fileCountDBEnvironmentProperties) {
        this(openFilesManager, cachesManager, memoryManager, fileCountDBEnvironmentProperties.getDataDirectory() + "server/");
    }

    public FileDataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(cachesManager, memoryManager);
        this.memoryManager = memoryManager;
        this.openFilesManager = openFilesManager;
        this.directory = directory;
        this.interfaces = new ArrayList<>();
        this.writeCleanFilesListThread = new WriteCleanFilesListThread();
        this.writeCleanFilesListThread.start();
    }

    @Override
    protected <T extends Object> DataInterface<T> createBaseDataInterface(final String nameOfSubset, final Class<T> objectClass, final Combinator<T> combinator) {
        FileDataInterface<T> result = new FileDataInterface<>(openFilesManager, combinator, objectClass, directory, nameOfSubset);
        memoryManager.registerMemoryGobbler(result);
        synchronized (interfaces) {
            interfaces.add(result);
        }
        return result;
    }

    @Override
    public void close() {
        this.writeCleanFilesListThread.terminateAndWait();
        super.close();
    }

    private class WriteCleanFilesListThread extends SafeThread {

        public WriteCleanFilesListThread() {
            super("WriteCleanFilesListThread", true);
        }

        @Override
        protected void runInt() throws Exception {
            while (!isTerminateRequested()) {
                for (int i = 0; i < interfaces.size(); i++) {
                    interfaces.get(i).writeCleanFilesListIfNecessary();
                }
                Utils.threadSleep(1000);
            }
        }
    }

}
