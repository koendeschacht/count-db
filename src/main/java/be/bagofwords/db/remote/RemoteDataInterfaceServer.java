package be.bagofwords.db.remote;

import be.bagofwords.application.SocketRequestHandler;
import be.bagofwords.application.SocketRequestHandlerFactory;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.memory.MemoryStatus;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.ReflectionUtils;
import be.bagofwords.util.SerializationUtils;
import be.bagofwords.util.SocketConnection;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static be.bagofwords.application.SocketServer.*;

public class RemoteDataInterfaceServer implements SocketRequestHandlerFactory {

    public static final String NAME = "RemoteDataInterfaceServer";

    private static final long CLONE_BATCH_SIZE_PRIMITIVE = 100000;
    private static final long CLONE_BATCH_SIZE_NON_PRIMITIVE = 100;

    private final DataInterfaceFactory dataInterfaceFactory;
    /*
        This list keeps references to the data interfaces created by this server, so they are not garbage collected when the last socket handler for that interface is closed.
        We want them to be kept in memory, so the cached values are also kept in memory, and they can be reused quickly when the next connection is created to that interface.

        We probably don't want to use this list to look-up things, because the list kept by the DataInterfaceFactory should be considered the 'master' version.
     */
    private final List<DataInterface> createdInterfaces;
    private final Object createNewInterfaceLock = new Object();
    private final MemoryManager memoryManager;

    public RemoteDataInterfaceServer(ApplicationContext context) {
        this.dataInterfaceFactory = context.getBean(DataInterfaceFactory.class);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.createdInterfaces = new ArrayList<>();
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public SocketRequestHandler createSocketRequestHandler(SocketConnection socketConnection) throws IOException {
        byte connectionTypeAsByte = socketConnection.readByte();
        if (connectionTypeAsByte < ConnectionType.values().length) {
            ConnectionType connectionType = ConnectionType.values()[connectionTypeAsByte];
            if (connectionType == ConnectionType.BATCH_READ_FROM_INTERFACE) {
                socketConnection.useLargeOutputBuffer();
            } else if (connectionType == ConnectionType.BATCH_WRITE_TO_INTERFACE) {
                socketConnection.useLargeInputBuffer();
            } else if (connectionType != ConnectionType.CONNECT_TO_INTERFACE) {
                throw new RuntimeException("Unknown connection type " + connectionTypeAsByte);
            }
        }
        return new DataInterfaceSocketRequestHandler(socketConnection);
    }

    public class DataInterfaceSocketRequestHandler extends SocketRequestHandler {

        private DataInterface dataInterface;
        private long startTime;
        private long totalNumberOfRequests;

        private DataInterfaceSocketRequestHandler(SocketConnection socketConnection) throws IOException {
            super(socketConnection);
        }

        private void prepareHandler() throws Exception {
            startTime = System.currentTimeMillis();
            String interfaceName = connection.readString();
            boolean isTemporary = connection.readBoolean();
            Class objectClass = readClass();
            Class combinatorClass = readClass();
            Combinator combinator = (Combinator) ReflectionUtils.createObject(combinatorClass);
            synchronized (createNewInterfaceLock) {
                dataInterface = findInterface(interfaceName);
                if (dataInterface != null) {
                    if (dataInterface.getCombinator().getClass() != combinator.getClass() || dataInterface.getObjectClass() != objectClass || dataInterface.isTemporaryDataInterface() != isTemporary) {
                        writeError(" Data interface " + interfaceName + " was already initialized!");
                    } else if (dataInterface.wasClosed()) {
                        writeError(" Data interface " + interfaceName + " was closed!");
                    }
                } else {
                    dataInterface = dataInterfaceFactory.createDataInterface(DatabaseCachingType.CACHED, interfaceName, objectClass, combinator, isTemporary);
                    createdInterfaces.add(dataInterface);
                }
            }
            setName(getName() + "_" + dataInterface.getName());
            connection.writeLong(LONG_OK);
            connection.flush();
        }

        private DataInterface findInterface(String interfaceName) {
            synchronized (dataInterfaceFactory.getAllInterfaces()) {
                for (DataInterfaceFactory.DataInterfaceReference reference : dataInterfaceFactory.getAllInterfaces()) {
                    if (reference.getSubsetName().equals(interfaceName)) {
                        DataInterface dataInterface = reference.get();
                        if (dataInterface != null) {
                            return dataInterface;
                        }
                    }
                }
            }
            return null;
        }

        @Override
        public void handleRequests() throws Exception {
            prepareHandler();
            connection.getOs().flush();
            boolean keepReadingCommands = true;
            while (keepReadingCommands && connection.isOpen()) {
                keepReadingCommands = handleRequest();
                totalNumberOfRequests++;
                connection.getOs().flush();
            }
        }

        private boolean handleRequest() throws Exception {
            Action action = readNextAction();
            if (action == Action.CLOSE_CONNECTION) {
                connection.close();
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
                } else if (action == Action.READ_CACHED_VALUES) {
                    handleReadCachedValues();
                } else {
                    writeError("Unkown action " + action);
                    return false;
                }
            }
            return true;
        }

        private void handleReadCachedValues() throws IOException {
            CloseableIterator<KeyValue> iterator = dataInterface.cachedValueIterator();
            writeValuesInBatches(iterator);
            iterator.close();
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
        public void reportUnexpectedError(Exception ex) {
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
            writeValuesInBatches(valueIt);
            valueIt.close();
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
            CloseableIterator<KeyValue> iterator = dataInterface.iterator();
            writeValuesInBatches(iterator);
            iterator.close();
        }

        private void writeValuesInBatches(CloseableIterator<KeyValue> iterator) throws IOException {
            //will write data in batches so we can compress key's and values separately
            List<Long> currentBatchKeys = new ArrayList<>();
            List<Object> currentBatchValues = new ArrayList<>();
            int widthOfObject = SerializationUtils.getWidth(dataInterface.getObjectClass());
            long batchSize = widthOfObject != -1 && widthOfObject < 16 ? CLONE_BATCH_SIZE_PRIMITIVE : CLONE_BATCH_SIZE_NON_PRIMITIVE;
            while (iterator.hasNext()) {
                KeyValue curr = iterator.next();
                currentBatchKeys.add(curr.getKey());
                currentBatchValues.add(curr.getValue());
                if (currentBatchKeys.size() >= batchSize || memoryManager.getMemoryStatus() != MemoryStatus.FREE) {
                    writeCurrentBatch(currentBatchKeys, currentBatchValues);
                    currentBatchKeys.clear();
                    currentBatchValues.clear();
                }
            }
            if (!currentBatchKeys.isEmpty()) {
                writeCurrentBatch(currentBatchKeys, currentBatchValues);
            }
            connection.writeLong(LONG_END);
            connection.flush();
        }

        private void writeCurrentBatch(List<Long> currentBatchKeys, List<Object> currentBatchValues) throws IOException {
            //write keys
            connection.writeLong(currentBatchKeys.size());
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (Long key : currentBatchKeys) {
                dos.writeLong(key);
            }
            dos.close();
            byte[] origKeys = bos.toByteArray();
            connection.writeByteArray(origKeys);

            //write values
            bos = new ByteArrayOutputStream();
            dos = new DataOutputStream(bos);
            for (Object value : currentBatchValues) {
                byte[] objectAsBytes = SerializationUtils.objectToBytesCheckForNull(value, dataInterface.getObjectClass());
                if (SerializationUtils.getWidth(dataInterface.getObjectClass()) == -1) {
                    dos.writeInt(objectAsBytes.length);
                }
                dos.write(objectAsBytes);
            }
            dos.close();
            byte[] origValues = bos.toByteArray();
            byte[] compressedValues = Snappy.compress(origValues);
            //            UI.write("Compressed values from " + origValues.length + " to " + compressedValues.length);
            connection.writeByteArray(compressedValues);
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

    }

    public enum Action {
        READVALUE, WRITEVALUE, READVALUES, READKEYS, WRITEVALUES, DROPALLDATA, CLOSE_CONNECTION, FLUSH,
        READALLVALUES, READ_CACHED_VALUES, APPROXIMATE_SIZE, MIGHT_CONTAIN, EXACT_SIZE, OPTMIZE_FOR_READING,
    }

    public enum ConnectionType {
        CONNECT_TO_INTERFACE, BATCH_WRITE_TO_INTERFACE, BATCH_READ_FROM_INTERFACE
    }

}
