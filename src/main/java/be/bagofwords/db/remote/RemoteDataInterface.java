package be.bagofwords.db.remote;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.UpdateListener;
import be.bagofwords.db.impl.UpdateListenerCollection;
import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.db.remote.RemoteDataInterfaceServer.Action;
import be.bagofwords.exec.RemoteObjectConfig;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.jobs.AsyncJobService;
import be.bagofwords.logging.Log;
import be.bagofwords.util.ExecutorServiceFactory;
import be.bagofwords.util.KeyValue;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static be.bagofwords.db.remote.Protocol.*;

public class RemoteDataInterface<T> extends BaseDataInterface<T> {

    private final static int MAX_NUM_OF_CONNECTIONS = 50;
    private final static long MAX_WAIT = 60 * 1000;

    private final String host;
    private final int port;
    private final List<Connection> smallBufferConnections;
    private final List<Connection> largeWriteBufferConnections;
    private final List<Connection> largeReadBufferConnections;
    private final ExecutorService executorService;
    private final UpdateListenerCollection<T> updateListenerCollection;

    public RemoteDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, String host, int port, boolean isTemporaryDataInterface, AsyncJobService asyncJobService) {
        super(name, objectClass, combinator, objectSerializer, isTemporaryDataInterface);
        this.host = host;
        this.port = port;
        this.smallBufferConnections = new ArrayList<>();
        this.largeReadBufferConnections = new ArrayList<>();
        this.largeWriteBufferConnections = new ArrayList<>();
        executorService = ExecutorServiceFactory.createExecutorService("remote_data_interface");
        asyncJobService.schedulePeriodicJob(() -> ifNotClosed(this::removeUnusedConnections), 1000);
        updateListenerCollection = new UpdateListenerCollection<>();
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
                    Connection newConn = new Connection(this, host, port, largeWriteBuffer, largeReadBuffer, connectionType);
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
            doAction(Action.READ_VALUE, connection);
            connection.writeLong(key);
            connection.flush();
            T value = readValue(connection);
            releaseConnection(connection);
            return value;
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException(e);
        }
    }

    private T readValue(Connection connection) throws IOException {
        int size = objectSerializer.getObjectSize();
        if (size == -1) {
            size = connection.readInt();
        }
        byte[] bytes = connection.readByteArray(size);
        DataStream ds = new DataStream(bytes);
        return objectSerializer.readValue(ds, size);
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
        return readLong(Action.APPROXIMATE_SIZE);
    }

    @Override
    public long exactSize() {
        return readLong(Action.EXACT_SIZE);
    }

    @Override
    public void write(long key, T value) {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(Action.WRITE_VALUE, connection);
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
        updateListenerCollection.dateUpdated(key, value);
    }

    @Override
    public void write(CloseableIterator<KeyValue<T>> entries) {
        Connection connection = null;
        try {
            connection = selectLargeWriteBufferConnection();
            doAction(Action.WRITE_VALUES, connection);
            while (entries.hasNext()) {
                KeyValue<T> entry = entries.next();
                connection.writeLong(entry.getKey());
                writeValue(entry.getValue(), connection);
                updateListenerCollection.dateUpdated(entry.getKey(), entry.getValue());
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
        } finally {
            entries.close();
        }
    }

    private void writeValue(T value, Connection connection) throws IOException {
        DataStream ds = new DataStream();
        objectSerializer.writeValue(value, ds);
        int objectSize = objectSerializer.getObjectSize();
        if (objectSize == -1) {
            connection.writeInt(ds.position);
        }
        connection.writeByteArray(ds.buffer, ds.position);
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(final CloseableIterator<Long> keyIterator) {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            Connection thisConnection = connection;
            doAction(Action.ITERATOR_WITH_KEY_ITERATOR, thisConnection);
            executorService.submit(() -> {
                try {
                    while (keyIterator.hasNext()) {
                        Long nextKey = keyIterator.next();
                        thisConnection.writeLong(nextKey);
                    }
                    thisConnection.writeLong(LONG_END);
                    thisConnection.flush();
                    keyIterator.close();
                } catch (Exception e) {
                    Log.e("Received exception while sending keys for read(..), for interface " + getName() + ". Closing connection. ", e);
                    dropConnection(thisConnection);
                }
            });
            return createKeyValueIterator(thisConnection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Received exception while sending keys for read(..) for interface " + getName(), e);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            doAction(Action.ITERATOR, connection);
            connection.flush();
            return createKeyValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(KeyFilter keyFilter) {
        Connection connection = null;
        try {
            RemoteObjectConfig remoteObjectConfig = RemoteObjectConfig.create(keyFilter).add(keyFilter.getClass());
            connection = selectLargeReadBufferConnection();
            doAction(Action.ITERATOR_WITH_KEY_FILTER, connection);
            connection.writeValue(remoteObjectConfig.pack());
            connection.flush();
            return createKeyValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    @Override
    public CloseableIterator<T> valueIterator(KeyFilter keyFilter) {
        Connection connection = null;
        try {
            RemoteObjectConfig remoteObjectConfig = RemoteObjectConfig.create(keyFilter).add(keyFilter.getClass());
            connection = selectLargeReadBufferConnection();
            doAction(Action.VALUES_ITERATOR_WITH_KEY_FILTER, connection);
            connection.writeValue(remoteObjectConfig.pack());
            connection.flush();
            return createValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(Predicate<T> valueFilter) {
        Connection connection = null;
        try {
            RemoteObjectConfig execConfig = RemoteObjectConfig.create(valueFilter).add(valueFilter.getClass());
            connection = selectLargeReadBufferConnection();
            doAction(Action.ITERATOR_WITH_VALUE_FILTER, connection);
            connection.writeValue(execConfig.pack());
            connection.flush();
            return createKeyValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    @Override
    public CloseableIterator<T> valueIterator(Predicate<T> valueFilter) {
        Connection connection = null;
        try {
            RemoteObjectConfig execConfig = RemoteObjectConfig.create(valueFilter).add(valueFilter.getClass());
            connection = selectLargeReadBufferConnection();
            doAction(Action.VALUES_ITERATOR_WITH_VALUE_FILTER, connection);
            connection.writeValue(execConfig.pack());
            connection.flush();
            return createValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    private CloseableIterator<KeyValue<T>> createKeyValueIterator(final Connection connection) {
        return new KeyValueSocketIterator<>(this, connection);
    }

    private CloseableIterator<T> createValueIterator(final Connection connection) {
        return new ValueSocketIterator<>(this, connection);
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        Connection connection = null;
        try {
            connection = selectLargeReadBufferConnection();
            doAction(Action.READ_KEYS, connection);
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
            return createKeyValueIterator(connection);
        } catch (Exception e) {
            dropConnection(connection);
            throw new RuntimeException("Failed to iterate over values from " + host + ":" + port, e);
        }
    }

    @Override
    public void dropAllData() {
        doSimpleAction(Action.DROP_ALL_DATA);
        updateListenerCollection.dataDropped();
    }

    private long readLong(Action action) {
        Connection connection = null;
        try {
            connection = selectSmallBufferConnection();
            doAction(action, connection);
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
    public synchronized void flush() {
        ifNotClosed(() -> doSimpleAction(Action.FLUSH));
        updateListenerCollection.dataFlushed();
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
    public DataInterface<T> getCoreDataInterface() {
        return this;
    }

    @Override
    public void registerUpdateListener(UpdateListener<T> updateListener) {
        updateListenerCollection.registerUpdateListener(updateListener);
    }

    void doAction(Action action, Connection connection) throws IOException {
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

    void releaseConnection(Connection connection) {
        if (connection != null) {
            connection.release();
        }
    }

    void dropConnection(Connection connection) {
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

}
