package be.bagofwords.db.remote;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.remote.RemoteDataInterfaceServer.Action;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.WrappedSocketConnection;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static be.bagofwords.application.BaseServer.*;

public class RemoteDataInterface<T> extends DataInterface<T> {

    private final static int MAX_NUM_OF_CONNECTIONS = 200;
    private final static long MAX_WAIT = 10000;

    private final String host;
    private final int port;
    private final List<Connection> connections;
    private final ExecutorService executorService;

    public RemoteDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, String host, int port) {
        super(name, objectClass, combinator);
        this.host = host;
        this.port = port;
        this.connections = new ArrayList<>();
        executorService = Executors.newFixedThreadPool(10);
    }

    private Connection selectConnection() throws IOException {
        Connection result = trySimpleSelect();
        if (result != null) {
            return result;
        } else {
            //Can we create an extra connection?
            if (connections.size() < MAX_NUM_OF_CONNECTIONS) {
                synchronized (connections) {
                    if (connections.size() < MAX_NUM_OF_CONNECTIONS) {
                        Connection newConn = new Connection(host, port);
                        connections.add(newConn);
                        newConn.setTaken(true);
                        return newConn;
                    }
                }
            }
            //Let's wait until a connection becomes available
            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < MAX_WAIT) {
                result = trySimpleSelect();
                if (result != null) {
                    return result;
                }
            }
        }
        throw new RuntimeException("Failed to reserve a connection!");
    }

    private Connection trySimpleSelect() {
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
    protected T readInt(long key) {
        Connection connection = null;
        try {
            connection = selectConnection();
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
            connection = selectConnection();
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
            connection = selectConnection();
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
            connection = selectConnection();
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
    protected void writeInt(long key, T value) {
        Connection connection = null;
        try {
            connection = selectConnection();
            doAction(Action.WRITEVALUE, connection);
            connection.writeLong(key);
            connection.writeValue(value, getObjectClass());
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
            connection = selectConnection();
            doAction(Action.WRITEVALUES, connection);
            while (entries.hasNext()) {
                KeyValue<T> entry = entries.next();
                connection.writeLong(entry.getKey());
                connection.writeValue(entry.getValue(), getObjectClass());
            }
            connection.writeLong(LONG_END);
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
    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        try {
            final Connection connection = new Connection(host, port);
            doAction(Action.READVALUES, connection);
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        while (keyIterator.hasNext()) {
                            Long nextKey = keyIterator.next();
                            connection.writeLong(nextKey);
                        }
                        connection.writeLong(LONG_END);
                        connection.flush();
                    } catch (Exception e) {
                        UI.writeError("Received exception while sending keys for read(..), for subset " + getName() + ". Closing connection. ", e);
                        IOUtils.closeQuietly(connection);
                    }
                }
            });
            return createNewKeyValueIterator(connection);
        } catch (Exception e) {
            throw new RuntimeException("Received exception while sending keys for read(..) for subset " + getName(), e);
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        try {
            final Connection connection = new Connection(host, port);
            doAction(Action.READALLVALUES, connection);
            connection.flush();
            return createNewKeyValueIterator(connection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableIterator<KeyValue<T>> createNewKeyValueIterator(final Connection connection) {
        return new CloseableIterator<KeyValue<T>>() {

            private KeyValue<T> next;

            {
                //Constructor
                findNext();
            }

            private void findNext() {
                if (!wasClosed()) {
                    try {
                        long key = connection.readLong();
                        if (key == LONG_END) {
                            next = null;
                            close();
                        } else if (key != LONG_ERROR) {
                            T value = connection.readValue(getObjectClass());
                            next = new KeyValue<>(key, value);
                        } else {
                            throw new RuntimeException("Unexpected response " + connection.readString());

                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    next = null;
                }
            }

            @Override
            public void closeInt() {
                synchronized (connection) {
                    if (connection.isOpen()) {
                        IOUtils.closeQuietly(connection);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> result = next;
                findNext();
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
        try {
            final Connection connection = new Connection(host, port);
            doAction(Action.READKEYS, connection);
            connection.flush();
            return new CloseableIterator<Long>() {

                private Long next;

                {
                    //Constructor
                    findNext();
                }

                private void findNext() {
                    try {
                        long key = connection.readLong();
                        if (key == LONG_END) {
                            //End
                            next = null;
                            close();
                        } else if (key != LONG_ERROR) {
                            next = key;
                        } else {
                            throw new RuntimeException("Unexpected response " + connection.readString());

                        }
                    } catch (Exception e) {
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
                    synchronized (connection) {
                        if (connection.isOpen()) {
                            IOUtils.closeQuietly(connection);
                        }
                    }
                }
            };
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropAllData() {
        doSimpleAction(Action.DROPALLDATA);
    }

    @Override
    public void flush() {
        doSimpleAction(Action.FLUSH);
    }

    @Override
    public void optimizeForReading() {
        doSimpleAction(Action.OPTMIZE_FOR_READING);
    }

    @Override
    protected void doClose() {
        try {
            flush();
        } catch (Exception e) {
            UI.writeError("Error while trying to flush data before close", e);
        }
        synchronized (connections) {
            for (Connection connection : connections) {
                IOUtils.closeQuietly(connection);
            }
            connections.clear();
        }
        executorService.shutdownNow();
    }

    @Override
    public DataInterface getImplementingDataInterface() {
        return null;
    }

    private void doAction(Action action, Connection connection) throws IOException {
        connection.writeByte((byte) action.ordinal());
    }


    private void doSimpleAction(Action action) {
        Connection connection = null;
        try {
            connection = selectConnection();
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
            synchronized (connections) {
                connections.remove(connection);
            }
        }
    }

    @Override
    public void valuesChanged(long[] keys) {
        notifyListenersOfChangedValues(keys);
    }

    private class Connection extends WrappedSocketConnection {

        private boolean isTaken;

        private Connection(String host, int port) throws IOException {
            this(host, port, false);
        }

        public Connection(String host, int port, boolean useLargeOutputBuffer) throws IOException {
            super(host, port, useLargeOutputBuffer);
            initializeSubset();
        }

        private void initializeSubset() throws IOException {
            writeByte((byte) Action.CONNECT_TO_INTERFACE.ordinal());
            writeString(getName());
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
        }

        public void release() {
            isTaken = false;
        }

        public void close() throws IOException {
            doAction(Action.CLOSE_CONNECTION, this);
            super.close();
        }
    }

}
