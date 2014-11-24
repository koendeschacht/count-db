package be.bagofwords.db.remote;

import be.bagofwords.application.BowTaskScheduler;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.remote.RemoteDataInterfaceServer.Action;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.ExecutorServiceFactory;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;
import be.bagofwords.util.WrappedSocketConnection;
import org.apache.commons.io.IOUtils;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static be.bagofwords.application.BaseServer.*;

public class RemoteDataInterface<T> extends DataInterface<T> {

    private final static int MAX_NUM_OF_CONNECTIONS = 50;
    private final static long MAX_WAIT = 60 * 1000;

    private final String host;
    private final int port;
    private final List<Connection> smallBufferConnections;
    private final List<Connection> largeWriteBufferConnections;
    private final List<Connection> largeReadBufferConnections;
    private final ExecutorService executorService;

    public RemoteDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, String host, int port, boolean isTemporaryDataInterface, BowTaskScheduler taskScheduler) {
        super(name, objectClass, combinator, isTemporaryDataInterface);
        this.host = host;
        this.port = port;
        this.smallBufferConnections = new ArrayList<>();
        this.largeReadBufferConnections = new ArrayList<>();
        this.largeWriteBufferConnections = new ArrayList<>();
        executorService = ExecutorServiceFactory.createExecutorService("remote_data_interface");
        taskScheduler.schedulePeriodicTask(() -> ifNotClosed(this::removeUnusedConnections), 1000);
    }

    private Connection selectSmallBufferConnection() throws IOException {
        return selectConnection(smallBufferConnections, false, false, RemoteDataInterfaceServer.ConnectionType.CONNECT_TO_INTERFACE);
    }

    private Connection selectLargeWriteBufferConnection() throws IOException {
        return selectConnection(largeWriteBufferConnections, true, false, RemoteDataInterfaceServer.ConnectionType.BATCH_WRITE_TO_INTERFACE);
    }

    private Connection selectLargeReadBufferConnection() throws IOException {
        return selectConnection(largeReadBufferConnections, false, true, RemoteDataInterfaceServer.ConnectionType.BATCH_READ_FROM_INTERFACE);
    }

    private Connection selectConnection(List<Connection> connections, boolean largeWriteBuffer, boolean largeReadBuffer, RemoteDataInterfaceServer.ConnectionType connectionType) throws IOException {
        Connection result = selectFreeConnection(connections);
        if (result != null) {
            return result;
        } else {
            //Can we create an extra connection?
            synchronized (connections) {
                if (connections.size() < MAX_NUM_OF_CONNECTIONS) {
                    Connection newConn = new Connection(host, port, largeWriteBuffer, largeReadBuffer, connectionType);
                    connections.add(newConn);
                    newConn.setTaken(true);
                    return newConn;
                }
            }
            //Let's wait until a connection becomes available
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < MAX_WAIT) {
                result = selectFreeConnection(connections);
                if (result != null) {
                    return result;
                }
            }
        }
        throw new RuntimeException("Failed to reserve a connection!");
    }

    private Connection selectFreeConnection(List<Connection> connections) {
        synchronized (connections) {
            for (Connection connection : connections) {
                if (!connection.isTaken()) {
                    connection.setTaken(true);
                    return connection;
                }
            }
        }
        return null;
    }

    @Override
    public T read(long key) {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(Action.READVALUE, connection);
            connection.writeLong(key);
            connection.flush();
            T value = connection.readValue(getObjectClass());
            releaseConnection(connection);
            return value;
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean mightContain(long key) {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(Action.MIGHT_CONTAIN, connection);
            connection.writeLong(key);
            connection.flush();
            boolean result = connection.readBoolean();
            releaseConnection(connection);
            return result;
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long apprSize() {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(Action.APPROXIMATE_SIZE, connection);
            connection.flush();
            long response = connection.readLong();
            if (response == LONG_OK) {
                long result = connection.readLong();
                releaseConnection(connection);
                return result;
            } else {
                dropConnection(connection);
                throw new RuntimeException("Unexpected error while reading approximate size " + connection.readString());
            }
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    @Override
    public long exactSize() {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(Action.EXACT_SIZE, connection);
            connection.flush();
            long response = connection.readLong();
            if (response == LONG_OK) {
                long result = connection.readLong();
                releaseConnection(connection);
                return result;
            } else {
                dropConnection(connection);
                throw new RuntimeException("Unexpected error while reading approximate size " + connection.readString());
            }
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(long key, T value) {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(Action.WRITEVALUE, connection);
            connection.writeLong(key);
            writeValue(value, connection);
            connection.flush();
            long response = connection.readLong();
            if (response != LONG_OK) {
                dropConnection(connection);
                throw new RuntimeException("Unexpected error while reading approximate size " + connection.readString());
            } else {
                releaseConnection(connection);
            }
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        Connection connection = null;
        try {
            connection = selectLargeWriteBufferConnection();
            doAction(Action.WRITEVALUES, connection);
            while (entries.hasNext()) {
                KeyValue<T> entry = entries.next();
                connection.writeLong(entry.getKey());
                writeValue(entry.getValue(), connection);
            }
            connection.writeLong(LONG_END);
            connection.flush();
            long response = connection.readLong();
            if (response != LONG_OK) {
                throw new RuntimeException("Unexpected error while reading approximate size " + connection.readString());
            }
            releaseConnection(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    private void writeValue(T value, Connection connection) throws IOException {
        connection.writeValue(value, getObjectClass());
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            Connection thisConnection = connection;
            doAction(Action.READVALUES, thisConnection);
            executorService.submit(() -> {
                try {
                    while (keyIterator.hasNext()) {
                        Long nextKey = keyIterator.next();
                        thisConnection.writeLong(nextKey);
                    }
                    thisConnection.writeLong(LONG_END);
                    thisConnection.flush();
                } catch (Exception e) {
                    UI.writeError("Received exception while sending keys for read(..), for subset " + getName() + ". Closing connection. ", e);
                    dropConnection(thisConnection);
                }
            });
            return createNewKeyValueIterator(thisConnection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Received exception while sending keys for read(..) for subset " + getName(), e);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            doAction(Action.READALLVALUES, connection);
            connection.flush();
            return createNewKeyValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    private CloseableIterator<KeyValue<T>> createNewKeyValueIterator(final Connection connection) {
        return new CloseableIterator<KeyValue<T>>() {

            private Iterator<KeyValue<T>> nextValues;
            private boolean readAllValuesFromConnection = false;

            {
                //Constructor
                findNextValues();
            }

            private synchronized void findNextValues() {
                if (!wasClosed()) {
                    try {
                        long numOfValues = connection.readLong();
                        if (numOfValues == LONG_END) {
                            nextValues = null;
                            readAllValuesFromConnection = true;
                        } else if (numOfValues != LONG_ERROR) {
                            byte[] keys = connection.readByteArray();
                            byte[] compressedValues = connection.readByteArray();
                            byte[] uncompressedValues = Snappy.uncompress(compressedValues);
                            DataInputStream keyIS = new DataInputStream(new ByteArrayInputStream(keys));
                            DataInputStream valueIS = new DataInputStream(new ByteArrayInputStream(uncompressedValues));
                            List<KeyValue<T>> nextValuesList = new ArrayList<>();
                            while (nextValuesList.size() < numOfValues) {
                                long key = keyIS.readLong();
                                int length = SerializationUtils.getWidth(getObjectClass());
                                if (length == -1) {
                                    length = valueIS.readInt();
                                }
                                byte[] objectAsBytes = new byte[length];
                                if (length > 0) {
                                    int bytesRead = valueIS.read(objectAsBytes);
                                    if (bytesRead < length) {
                                        throw new RuntimeException("Read " + bytesRead + " bytes, expected " + length);
                                    }
                                }
                                T value = SerializationUtils.bytesToObjectCheckForNull(objectAsBytes, getObjectClass());
                                nextValuesList.add(new KeyValue<>(key, value));
                            }
                            if (nextValuesList.isEmpty()) {
                                throw new RuntimeException("Received zero values! numOfValues=" + numOfValues);
                            }
                            nextValues = nextValuesList.iterator();
                        } else {
                            throw new RuntimeException("Unexpected response " + connection.readString());

                        }
                    } catch (Exception e) {
                        dropConnection(connection);
                        throw new RuntimeException(e);
                    }
                } else {
                    nextValues = null;
                }
            }

            @Override
            public void closeInt() {
                if (readAllValuesFromConnection) {
                    releaseConnection(connection);
                } else {
                    //server will still be sending data through this connection, so it can not be reused.
                    dropConnection(connection);
                }
            }

            @Override
            public boolean hasNext() {
                return nextValues != null;
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> result = nextValues.next();
                if (!nextValues.hasNext()) {
                    findNextValues();
                }
                return result;
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not implemented");
            }

        };
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            doAction(Action.READKEYS, connection);
            connection.flush();
            Connection thisConnection = connection;
            return new CloseableIterator<Long>() {

                private Long next;
                private boolean readLastValue = false;

                {
                    //Constructor
                    findNext();
                }

                private void findNext() {
                    try {
                        long key = thisConnection.readLong();
                        if (key == LONG_END) {
                            //End
                            next = null;
                            readLastValue = true;
                        } else if (key != LONG_ERROR) {
                            next = key;
                        } else {
                            throw new RuntimeException("Unexpected response " + thisConnection.readString());

                        }
                    } catch (Exception e) {
                        dropConnection(thisConnection);
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public boolean hasNext() {
                    return next != null;
                }

                @Override
                public Long next() {
                    Long result = next;
                    findNext();
                    return result;
                }

                @Override
                public void closeInt() {
                    if (readLastValue) {
                        releaseConnection(thisConnection);
                    } else {
                        dropConnection(thisConnection);
                    }
                }
            };
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> cachedValueIterator() {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            doAction(Action.READ_CACHED_VALUES, connection);
            connection.flush();
            return createNewKeyValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    @Override
    public void dropAllData() {
        doSimpleAction(Action.DROPALLDATA);
    }

    @Override
    public synchronized void flush() {
        ifNotClosed(() -> doSimpleAction(Action.FLUSH));
    }

    private void removeUnusedConnections() {
        removeUnusedConnections(smallBufferConnections);
        removeUnusedConnections(largeReadBufferConnections);
        removeUnusedConnections(largeWriteBufferConnections);
    }

    private void removeUnusedConnections(List<Connection> connections) {
        synchronized (connections) {
            List<Connection> unusedConnections = connections.stream().filter(connection -> (!connection.isTaken() && System.currentTimeMillis() - connection.getLastUsage() > 60 * 1000) || !connection.isOpen()).collect(Collectors.toList());
            for (Connection unusedConnection : unusedConnections) {
                dropConnection(unusedConnection);
            }
        }
    }

    @Override
    public void optimizeForReading() {
        doSimpleAction(Action.OPTMIZE_FOR_READING);
    }

    @Override
    protected void doClose() {
        dropConnections(smallBufferConnections);
        dropConnections(largeWriteBufferConnections);
        dropConnections(largeReadBufferConnections);
        executorService.shutdownNow();
    }

    private void dropConnections(List<Connection> connections) {
        synchronized (connections) {
            for (Connection connection : connections) {
                IOUtils.closeQuietly(connection);
            }
            connections.clear();
        }
    }

    @Override
    public DataInterface getCoreDataInterface() {
        return this;
    }

    private void doAction(Action action, Connection connection) throws IOException {
        connection.writeByte((byte) action.ordinal());
    }


    private void doSimpleAction(Action action) {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(action, connection);
            connection.flush();
            long response = connection.readLong();
            if (response != LONG_OK) {
                dropConnection(connection);
                throw new RuntimeException("Unexpected response for action " + action + " " + connection.readString());
            } else {
                releaseConnection(connection);
            }
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    private void releaseConnection(Connection connection) {
        if (connection != null) {
            connection.release();
        }
    }

    private void dropConnection(Connection connection) {
        if (connection != null) {
            IOUtils.closeQuietly(connection);
            synchronized (smallBufferConnections) {
                smallBufferConnections.remove(connection);
            }
            synchronized (largeReadBufferConnections) {
                largeReadBufferConnections.remove(connection);
            }
            synchronized (largeWriteBufferConnections) {
                largeWriteBufferConnections.remove(connection);
            }
        }
    }

    private class Connection extends WrappedSocketConnection {

        private boolean isTaken;
        private long lastUsage;

        public Connection(String host, int port, boolean useLargeOutputBuffer, boolean useLargeInputBuffer, RemoteDataInterfaceServer.ConnectionType connectionType) throws IOException {
            super(host, port, useLargeOutputBuffer, useLargeInputBuffer);
            initializeSubset(connectionType);
        }

        private void initializeSubset(RemoteDataInterfaceServer.ConnectionType connectionType) throws IOException {
            writeByte((byte) connectionType.ordinal());
            writeString(getName());
            writeBoolean(isTemporaryDataInterface());
            writeString(getObjectClass().getCanonicalName());
            writeString(getCombinator().getClass().getCanonicalName());
            flush();
            long response = readLong();
            if (response == LONG_ERROR) {
                String errorMessage = readString();
                throw new RuntimeException("Received unexpected message while initializing subset " + errorMessage);
            }
        }

        private boolean isTaken() {
            return isTaken;
        }

        private void setTaken(boolean taken) {
            isTaken = taken;
            lastUsage = System.currentTimeMillis();
        }

        public void release() {
            isTaken = false;
            lastUsage = System.currentTimeMillis();
        }

        public long getLastUsage() {
            return lastUsage;
        }

        public void close() throws IOException {
            doAction(Action.CLOSE_CONNECTION, this);
            super.close();
        }
    }

}
