package be.bow.db;

import be.bow.application.memory.MemoryManager;
import be.bow.application.memory.MemoryStatus;
import be.bow.ui.UI;
import be.bow.util.SafeThread;
import be.bow.util.Utils;

public class FlushDataInterfacesThread extends SafeThread {

    public static final long TIME_BETWEEN_FLUSHES = 1000; //Flush data interfaces every second

    private final DataInterfaceFactory dataInterfaceFactory;
    private final MemoryManager memoryManager;

    public FlushDataInterfacesThread(DataInterfaceFactory dataInterfaceFactory, MemoryManager memoryManager) {
        super("FlushWriteBuffer", false);
        this.dataInterfaceFactory = dataInterfaceFactory;
        this.memoryManager = memoryManager;
    }

    @Override
    public void runInt() {
        while (!isTerminateRequested()) {
            try {
                for (DataInterface dataInterface : dataInterfaceFactory.getAllInterfaces()) {
                    dataInterface.flush();
                }
            } catch (Throwable t) {
                UI.writeError("Received exception while flushing write buffers!", t);
            }
            long timeToSleep = TIME_BETWEEN_FLUSHES;
            if (memoryManager.getMemoryStatus() == MemoryStatus.CRITICAL) {
                timeToSleep = TIME_BETWEEN_FLUSHES / 10;
            }
            Utils.threadSleep(timeToSleep);
        }
    }
}
