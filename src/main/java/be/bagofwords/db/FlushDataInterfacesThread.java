package be.bagofwords.db;

import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.application.memory.MemoryStatus;
import be.bagofwords.ui.UI;
import be.bagofwords.util.SafeThread;
import be.bagofwords.util.Utils;

import java.util.ArrayList;
import java.util.List;

public class FlushDataInterfacesThread extends SafeThread {

    public static final long TIME_BETWEEN_FLUSHES = 1000; //Flush data interfaces every second

    private final DataInterfaceFactory dataInterfaceFactory;
    private final MemoryManager memoryManager;

    public FlushDataInterfacesThread(DataInterfaceFactory dataInterfaceFactory, MemoryManager memoryManager) {
        super("FlushWriteBuffer", true);
        this.dataInterfaceFactory = dataInterfaceFactory;
        this.memoryManager = memoryManager;
    }

    @Override
    public void runInt() {
        while (!isTerminateRequested()) {
            long timeBeforeFlush = System.currentTimeMillis();
            List<DataInterface> currentInterfaces;
            synchronized (dataInterfaceFactory.getAllInterfaces()) {
                currentInterfaces = new ArrayList<>(dataInterfaceFactory.getAllInterfaces());
            }
            for (DataInterface dataInterface : currentInterfaces) {
                try {
                    dataInterface.flushIfNotClosed();
                } catch (Throwable t) {
                    UI.writeError("Received exception while flushing write buffer for data interface " + dataInterface.getName() + ". Will close this interface.", t);
                    //we probably lost some data in the flush, to make sure that other threads know about the problems with
                    //this data interface, we close it.
                    try {
                        dataInterface.close();
                    } catch (Throwable t2) {
                        UI.writeError("Failed to close " + dataInterface.getName(), t2);
                    }
                }
            }
            long timeToSleepBetweenFlushes = TIME_BETWEEN_FLUSHES;
            if (memoryManager.getMemoryStatus() == MemoryStatus.CRITICAL) {
                timeToSleepBetweenFlushes = 0;
            }
            long actualTimeToSleep = timeBeforeFlush - System.currentTimeMillis() + timeToSleepBetweenFlushes;
            if (actualTimeToSleep > 0) {
                Utils.threadSleep(actualTimeToSleep);
            }
        }
    }
}
