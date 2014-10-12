package be.bagofwords.db.remote;

import be.bagofwords.application.BaseServer;
import be.bagofwords.application.annotations.BowComponent;
import be.bagofwords.application.status.StatusViewable;
import be.bagofwords.db.ChangedValuesListener;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.NumUtils;
import be.bagofwords.util.ReflectionUtils;
import be.bagofwords.util.WrappedSocketConnection;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.*;

@BowComponent
public class RemoteDataInterfaceServer extends BaseServer implements StatusViewable {

    private final DataInterfaceFactory dataInterfaceFactory;
    private final List<WrappedSocketConnection> listenToChangesConnections;

    @Autowired
    public RemoteDataInterfaceServer(DataInterfaceFactory dataInterfaceFactory, RemoteCountDBEnvironmentProperties properties) throws IOException {
        super("RemoteDataInterfaceServer", properties.getDataInterfaceServerPort());
        this.dataInterfaceFactory = dataInterfaceFactory;
        this.listenToChangesConnections = new ArrayList<>();
    }

    @Override
    protected SocketRequestHandler createSocketRequestHandler(WrappedSocketConnection connection) throws IOException {
        Action action = Action.values()[connection.readByte()];
        if (action == Action.CONNECT_TO_INTERFACE) {
            return new DataInterfaceSocketRequestHandler(connection);
        } else if (action == Action.LISTEN_TO_CHANGES) {
            synchronized (listenToChangesConnections) {
                listenToChangesConnections.add(connection);
            }
            return null;
        } else {
            throw new RuntimeException("Unknown action " + action);
        }
    }

    private void valuesChangedForInterface(String interfaceName, long[] keys) {
        synchronized (listenToChangesConnections) {
            for (int i = 0; i < listenToChangesConnections.size(); i++) {
                WrappedSocketConnection connection = listenToChangesConnections.get(i);
                try {
                    connection.writeString(interfaceName);
                    connection.writeInt(keys.length);
                    for (Long key : keys) {
                        connection.writeLong(key);
                    }
                    connection.flush();
                    long response = connection.readLong();
                    if (response != LONG_OK) {
                        throw new RuntimeException("Unexpected response " + response + " from " + connection.getInetAddress());
                    }
                } catch (IOException exp) {
                    IOUtils.closeQuietly(connection);
                    listenToChangesConnections.remove(i--);
                }
            }
        }
    }

    public class DataInterfaceSocketRequestHandler extends BaseServer.SocketRequestHandler {

        private DataInterface dataInterface;
        private long startTime;
        private long totalNumberOfRequests;

        private DataInterfaceSocketRequestHandler(WrappedSocketConnection wrappedSocketConnection) throws IOException {
            super(wrappedSocketConnection);
        }

        private void prepareHandler() throws Exception {
            startTime = System.currentTimeMillis();
            final String interfaceName = connection.readString();
            Class objectClass = readClass();
            Class combinatorClass = readClass();
            Combinator combinator = (Combinator) ReflectionUtils.createObject(combinatorClass);
            synchronized (dataInterfaceFactory.getAllInterfaces()) {
                dataInterface = findInterface(dataInterfaceFactory.getAllInterfaces(), interfaceName);
                if (dataInterface != null) {
                    if (dataInterface.getCombinator().getClass() != combinator.getClass() || dataInterface.getObjectClass() != objectClass) {
                        writeError(" Data interface " + interfaceName + " was already initialized!");
                    } else if (dataInterface.wasClosed()) {
                        writeError(" Data interface " + interfaceName + " was closed!");
                    }
                } else {
                    dataInterface = dataInterfaceFactory.createDataInterface(DatabaseCachingType.CACHED, interfaceName, objectClass, combinator);
                    dataInterface.registerListener(new ChangedValuesListener() {
                        @Override
                        public void valuesChanged(long[] keys) {
                            valuesChangedForInterface(interfaceName, keys);
                        }
                    });
                }
            }
            setName(getName() + "_" + dataInterface.getName());
            connection.writeLong(LONG_OK);
            connection.flush();
        }

        private DataInterface findInterface(List<DataInterface> allInterfaces, String interfaceName) {
            for (DataInterface dataInterface : allInterfaces) {
                if (dataInterface.getName().equals(interfaceName)) {
                    return dataInterface;
                }
            }
            return null;
        }

        @Override
        protected void handleRequests() throws Exception {
            prepareHandler();
            connection.getOs().flush();
            boolean keepReadingCommands = true;
            while (keepReadingCommands && !isTerminateRequested()) {
                keepReadingCommands = handleRequest();
                totalNumberOfRequests++;
                connection.getOs().flush();
            }
        }

        private boolean handleRequest() throws Exception {
            Action action = readNextAction();
            if (action == Action.CLOSE_CONNECTION) {
                terminate();
            } else {
                if (action == Action.EXACT_SIZE) {
                    handleExactSize();
                } else if (action == Action.APPROXIMATE_SIZE) {
                    handleApproximateSize();
                } else if (action == Action.READVALUE) {
                    handleReadValue();
                } else if (action == Action.WRITEVALUE) {
                    handleWriteValue();
                } else if (action == Action.READALLVALUES) {
                    handleReadAllValues();
                } else if (action == Action.READVALUES) {
                    handleReadValues();
                } else if (action == Action.WRITEVALUES) {
                    handleWriteValues();
                } else if (action == Action.READKEYS) {
                    handleReadKeys();
                } else if (action == Action.DROPALLDATA) {
                    handleDropAllData();
                } else if (action == Action.FLUSH) {
                    handleFlush();
                } else if (action == Action.MIGHT_CONTAIN) {
                    handleMightContain();
                } else if (action == Action.OPTMIZE_FOR_READING) {
                    handleOptimizeForReading();
                } else {
                    writeError("Unkown action " + action);
                    return false;
                }
            }
            return true;
        }

        private Action readNextAction() throws IOException {
            byte actionAsByte = connection.readByte();
            return Action.values()[actionAsByte];
        }

        @Override
        public long getTotalNumberOfRequests() {
            return totalNumberOfRequests;
        }

        @Override
        protected void reportUnexpectedError(Exception ex) {
            if (dataInterface != null) {
                UI.writeError("Unexpected exception in request handler for data interface " + dataInterface.getName(), ex);
            } else {
                UI.writeError("Unexpected exception in request handler", ex);
            }
        }

        public DataInterface getDataInterface() {
            return dataInterface;
        }

        public long getStartTime() {
            return startTime;
        }

        private void handleReadValues() throws IOException {
            CloseableIterator<KeyValue> valueIt = dataInterface.iterator(IterableUtils.iterator(new SimpleIterator<Long>() {
                @Override
                public Long next() throws Exception {
                    return connection.readLong();
                }
            }, LONG_END));
            try {
                while (valueIt.hasNext()) {
                    KeyValue value = valueIt.next();
                    connection.writeLong(value.getKey());
                    connection.writeValue(value.getValue(), dataInterface.getObjectClass());
                }
                connection.writeLong(LONG_END);
            } finally {
                IOUtils.closeQuietly(valueIt);
            }
        }

        private void writeError(String errorMessage) throws IOException {
            connection.writeLong(LONG_ERROR);
            connection.writeString(errorMessage);
        }

        private void handleFlush() throws IOException {
            dataInterface.flush();
            connection.writeLong(LONG_OK);
        }

        private void handleApproximateSize() throws IOException {
            long appSize = dataInterface.apprSize();
            connection.writeLong(LONG_OK);
            connection.writeLong(appSize);
        }

        private void handleExactSize() throws IOException {
            long exactSize = dataInterface.exactSize();
            connection.writeLong(LONG_OK);
            connection.writeLong(exactSize);
        }

        private void handleDropAllData() throws IOException {
            dataInterface.dropAllData();
            connection.writeLong(LONG_OK);
        }

        private void handleOptimizeForReading() throws IOException {
            dataInterface.optimizeForReading();
            connection.writeLong(LONG_OK);
        }

        private void handleWriteValues() throws IOException {

            dataInterface.write(new Iterator<KeyValue>() {

                private KeyValue nextValue;

                {
                    //psuedo constructor
                    readNextValue();
                }

                private void readNextValue() {
                    try {
                        long key = connection.readLong();
                        if (key == LONG_END) {
                            nextValue = null;
                        } else {
                            Object value = connection.readValue(dataInterface.getObjectClass());
                            nextValue = new KeyValue(key, value);
                        }
                    } catch (IOException exp) {
                        throw new RuntimeException("Received exception while reading list of values", exp);
                    }
                }

                @Override
                public boolean hasNext() {
                    return nextValue != null;
                }

                @Override
                public KeyValue next() {
                    KeyValue result = nextValue;
                    readNextValue();
                    return result;
                }

                @Override
                public void remove() {
                    throw new RuntimeException("Not implemented!");
                }
            });

            connection.writeLong(LONG_OK);
        }

        private void handleReadKeys() throws IOException {
            CloseableIterator<Long> it = dataInterface.keyIterator();
            while (it.hasNext()) {
                Long key = it.next();
                connection.writeLong(key);
            }
            it.close();
            connection.writeLong(LONG_END);
        }

        private void handleReadAllValues() throws IOException {
            CloseableIterator<KeyValue> it = dataInterface.iterator();
            try {
                while (it.hasNext()) {
                    KeyValue next = it.next();
                    Object valueToWrite = next.getValue();
                    long key = next.getKey();
                    if (key == LONG_END || key == LONG_ERROR || key == LONG_OK) {
                        throw new RuntimeException("Unexpected key " + key + " in dataInterface " + dataInterface.getName());
                    }
                    connection.writeLong(key);
                    connection.writeValue(valueToWrite, dataInterface.getObjectClass());
                }
                connection.writeLong(LONG_END);
            } finally {
                IOUtils.closeQuietly(it);
            }
        }

        private void handleReadValue() throws IOException {
            long key = connection.readLong();
            Object value = dataInterface.read(key);
            connection.writeValue(value, dataInterface.getObjectClass());
        }

        private void handleMightContain() throws IOException {
            long key = connection.readLong();
            boolean mightContain = dataInterface.mightContain(key);
            connection.writeBoolean(mightContain);
        }


        private void handleWriteValue() throws IOException {
            long key = connection.readLong();
            Object value = connection.readValue(dataInterface.getObjectClass());
            dataInterface.write(key, value);
            connection.writeLong(LONG_OK);
        }

        private Class readClass() throws IOException, ClassNotFoundException {
            String className = connection.readString();
            return Class.forName(className);
        }

        @Override
        public void doTerminate() {
            IOUtils.closeQuietly(connection);
        }
    }


    public static enum Action {
        READVALUE, WRITEVALUE, READVALUES, READKEYS, WRITEVALUES, DROPALLDATA, CLOSE_CONNECTION, FLUSH, READALLVALUES, APPROXIMATE_SIZE, MIGHT_CONTAIN, EXACT_SIZE, LISTEN_TO_CHANGES, CONNECT_TO_INTERFACE, OPTMIZE_FOR_READING
    }

    @Override
    public void doTerminate() {
        super.doTerminate();
    }

    @Override
    public void printHtmlStatus(StringBuilder sb) {
        sb.append("<h1>Printing database server statistics</h1>");
        ln(sb, "<table>");
        ln(sb, "<tr><td>Used memory is </td><td>" + UI.getMemoryUsage() + "</td></tr>");
        ln(sb, "<tr><td>Total number of connections </td><td>" + getTotalNumberOfConnections() + "</td></tr>");
        List<RemoteDataInterfaceServer.SocketRequestHandler> runningRequestHandlers = getRunningRequestHandlers();
        ln(sb, "<tr><td>Current number of handlers </td><td>" + runningRequestHandlers.size() + "</td></tr>");
        List<RemoteDataInterfaceServer.SocketRequestHandler> sortedRequestHandlers;
        synchronized (runningRequestHandlers) {
            sortedRequestHandlers = new ArrayList<>(runningRequestHandlers);
        }
        Collections.sort(sortedRequestHandlers, new Comparator<RemoteDataInterfaceServer.SocketRequestHandler>() {
            @Override
            public int compare(RemoteDataInterfaceServer.SocketRequestHandler o1, RemoteDataInterfaceServer.SocketRequestHandler o2) {
                return -Double.compare(o1.getTotalNumberOfRequests(), o2.getTotalNumberOfRequests());
            }
        });
        for (int i = 0; i < sortedRequestHandlers.size(); i++) {
            DataInterfaceSocketRequestHandler handler = (DataInterfaceSocketRequestHandler) sortedRequestHandlers.get(i);
            ln(sb, "<tr><td>" + i + " subset </td><td>" + handler.getDataInterface().getName() + "</td></tr>");
            ln(sb, "<tr><td>" + i + " Started at </td><td>" + new Date(handler.getStartTime()) + "</td></tr>");
            ln(sb, "<tr><td>" + i + " Total number of requests </td><td>" + handler.getTotalNumberOfRequests() + "</td></tr>");
            double requestsPerSec = handler.getTotalNumberOfRequests() * 1000.0 / (System.currentTimeMillis() - handler.getStartTime());
            ln(sb, "<tr><td>" + i + " Average requests/s</td><td>" + NumUtils.fmt(requestsPerSec) + "</td></tr>");
        }
        ln(sb, "</table>");
    }

    private void ln(StringBuilder sb, String s) {
        sb.append(s);
        sb.append("\n");
    }

}
