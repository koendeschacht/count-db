package be.bow.db.filedb;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.combinator.Combinator;
import be.bow.util.SafeThread;
import be.bow.util.Utils;

import java.util.ArrayList;
import java.util.List;

public class FileDataInterfaceFactory extends DataInterfaceFactory {

    private final MemoryManager memoryManager;
    private final OpenFilesManager openFilesManager;
    private final String directory;
    private final List<FileDataInterface> interfaces;
    private final WriteCleanFilesListThread writeCleanFilesListThread;

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
        this.writeCleanFilesListThread.close();
        super.close();
    }

    private class WriteCleanFilesListThread extends SafeThread {

        public WriteCleanFilesListThread() {
            super("WriteCleanFilesListThread", false);
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
