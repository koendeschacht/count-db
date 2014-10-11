package be.bagofwords.db.remote;

import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.SafeThread;
import be.bagofwords.util.WrappedSocketConnection;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.net.SocketException;
import java.util.HashMap;
import java.util.Map;

import static be.bagofwords.application.BaseServer.LONG_OK;

public class RemoteDatabaseInterfaceFactory extends DataInterfaceFactory {

    private final String host;
    private final int port;
    private Map<String, DataInterface> dataInterfaceMap;
    private ChangedValueListenerThread changedValueListenerThread;

    @Autowired
    public RemoteDatabaseInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, RemoteCountDBEnvironmentProperties environmentProperties) {
        this(cachesManager, memoryManager, environmentProperties.getDatabaseServerAddress(), environmentProperties.getDataInterfaceServerPort());
    }

    public RemoteDatabaseInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, String host, int port) {
        super(cachesManager, memoryManager);
        this.host = host;
        this.port = port;
        this.dataInterfaceMap = new HashMap<>();
    }

    @Override
    protected synchronized <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        if (changedValueListenerThread == null) {
            synchronized (this) {
                try {
                    this.changedValueListenerThread = new ChangedValueListenerThread();
                    this.changedValueListenerThread.start();
                } catch (IOException e) {
                    throw new RuntimeException("Unexpected exception while starting changedValueListenerThread", e);
                }
            }
        }
        DataInterface result = new RemoteDataInterface<>(nameOfSubset, objectClass, combinator, host, port);
        dataInterfaceMap.put(nameOfSubset, result);
        return result;
    }

    @Override
    public synchronized void terminate() {
        if (changedValueListenerThread != null) {
            changedValueListenerThread.terminateAndWaitForFinish();
        }
        super.terminate();
    }

    private class ChangedValueListenerThread extends SafeThread {

        private WrappedSocketConnection connection;

        public ChangedValueListenerThread() throws IOException {
            super("ChangedValueListener", false);
            connection = new WrappedSocketConnection(host, port);
        }

        @Override
        protected void runInt() throws Exception {
            connection.writeByte((byte) RemoteDataInterfaceServer.Action.LISTEN_TO_CHANGES.ordinal());
            connection.flush();
            try {
                while (!isTerminateRequested()) {
                    String interfaceName = connection.readString();
                    int numOfKeys = connection.readInt();
                    long[] keys = new long[numOfKeys];
                    for (int i = 0; i < numOfKeys; i++) {
                        keys[i] = connection.readLong();
                    }
                    DataInterface dataInterface = dataInterfaceMap.get(interfaceName);
                    if (dataInterface != null) {
                        dataInterface.notifyListenersOfChangedValues(keys);
                    }
                    connection.writeLong(LONG_OK);
                    connection.flush();
                }
            } catch (SocketException exp) {
                if (!exp.getMessage().equals("Socket closed")) {
                    UI.writeError("Error in ChangedValueListener", exp);
                }
            }
            IOUtils.closeQuietly(connection);
        }

        @Override
        protected void doTerminate() {
            IOUtils.closeQuietly(connection);
        }
    }
}
