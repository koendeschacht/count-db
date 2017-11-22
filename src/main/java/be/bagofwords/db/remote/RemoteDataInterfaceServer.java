package be.bagofwords.db.remote;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.DataStreamUtils;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.exec.PackedRemoteObject;
import be.bagofwords.exec.RemoteObjectUtil;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.exec.PackedRemoteObject;
import be.bagofwords.exec.RemoteObjectUtil;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.memory.MemoryStatus;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;
import be.bagofwords.util.SocketConnection;
import be.bagofwords.web.SocketRequestHandler;
import be.bagofwords.web.SocketRequestHandlerFactory;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import static be.bagofwords.db.remote.Protocol.*;

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
    private final RemoteObjectService remoteObjectService;

    public RemoteDataInterfaceServer(ApplicationContext context) {
        this.dataInterfaceFactory = context.getBean(DataInterfaceFactory.class);
        this.memoryManager = context.getBean(MemoryManager.class);
        this.remoteObjectService = context.getBean(RemoteObjectService.class);
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
            PackedRemoteObject packedRemoteObject = connection.readValue(PackedRemoteObject.class);
            Combinator combinator = (Combinator) remoteObjectService.loadObject(packedRemoteObject);
            packedRemoteObject = connection.readValue(PackedRemoteObject.class);
            ObjectSerializer objectSerializer = (ObjectSerializer) remoteObjectService.loadObject(packedRemoteObject);
            synchronized (createNewInterfaceLock) {
                dataInterface = findInterface(interfaceName);
                if (dataInterface != null) {
                    if (dataInterface.getCombinator().getClass() != combinator.getClass() || dataInterface.getObjectClass() != objectClass || dataInterface.isTemporaryDataInterface() != isTemporary) {
                        writeError(" Data interface " + interfaceName + " was already initialized!");
                    } else if (dataInterface.wasClosed()) {
                        writeError(" Data interface " + interfaceName + " was closed!");
                    }
                } else {
                    dataInterface = dataInterfaceFactory.dataInterface(interfaceName, objectClass).combinator(combinator).serializer(objectSerializer).temporary(isTemporary).create();
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
                    if (reference.getName().equals(interfaceName)) {
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
            try {
                prepareHandler();
                connection.getOs().flush();
                boolean keepReadingCommands = true;
                while (keepReadingCommands && connection.isOpen()) {
                    keepReadingCommands = handleRequest();
                    totalNumberOfRequests++;
                    connection.getOs().flush();
                }
            } catch (Exception exp) {
                if (isUnexpectedError(exp)) {
                    Log.i("Unexpected exception while handling remote data interface requests", exp);
                    writeError("Unexpected error " + exp.getMessage());
                }
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
                } else if (action == Action.READ_VALUE) {
                    handleReadValue();
                } else if (action == Action.WRITE_VALUE) {
                    handleWriteValue();
                } else if (action == Action.ITERATOR) {
                    handleIterator();
                } else if (action == Action.ITERATOR_WITH_KEY_ITERATOR) {
                    handleIteratorWithKeyIterator();
                } else if (action == Action.WRITE_VALUES) {
                    handleWriteValues();
                } else if (action == Action.READ_KEYS) {
                    handleReadKeys();
                } else if (action == Action.DROP_ALL_DATA) {
                    handleDropAllData();
                } else if (action == Action.FLUSH) {
                    handleFlush();
                } else if (action == Action.MIGHT_CONTAIN) {
                    handleMightContain();
                } else if (action == Action.OPTMIZE_FOR_READING) {
                    handleOptimizeForReading();
                } else if (action == Action.READ_CACHED_VALUES) {
                    handleReadCachedValues();
                } else if (action == Action.ITERATOR_WITH_KEY_FILTER) {
                    handleIteratorWithKeyFilter();
                } else if (action == Action.VALUES_ITERATOR_WITH_KEY_FILTER) {
                    handleValuesIteratorWithKeyFilter();
                } else if (action == Action.ITERATOR_WITH_VALUE_FILTER) {
                    handleIteratorWithValueFilter();
                } else if (action == Action.VALUES_ITERATOR_WITH_VALUE_FILTER) {
                    handleValuesIteratorWithValueFilter();
                } else {
                    writeError("Unkown action " + action);
                    return false;
                }
            }
            return true;
        }

        private void handleIteratorWithKeyFilter() throws IOException {
            PackedRemoteObject packedRemoteObject = connection.readValue(PackedRemoteObject.class);
            KeyFilter filter = (KeyFilter) RemoteObjectUtil.loadObject(packedRemoteObject);
            CloseableIterator<KeyValue> iterator = dataInterface.iterator(filter);
            writeKeyValuesInBatches(iterator);
            iterator.close();
        }

        private void handleValuesIteratorWithKeyFilter() throws IOException {
            PackedRemoteObject packedRemoteObject = connection.readValue(PackedRemoteObject.class);
            KeyFilter filter = (KeyFilter) RemoteObjectUtil.loadObject(packedRemoteObject);
            CloseableIterator iterator = dataInterface.valueIterator(filter);
            writeValuesInBatches(iterator);
            iterator.close();
        }

        private void handleIteratorWithValueFilter() throws IOException {
            PackedRemoteObject packedRemoteObject = connection.readValue(PackedRemoteObject.class);
            Predicate filter = (Predicate) RemoteObjectUtil.loadObject(packedRemoteObject);
            CloseableIterator<KeyValue> iterator = dataInterface.iterator(filter);
            writeKeyValuesInBatches(iterator);
            iterator.close();
        }

        private void handleValuesIteratorWithValueFilter() throws IOException {
            PackedRemoteObject packedRemoteObject = connection.readValue(PackedRemoteObject.class);
            Predicate filter = (Predicate) RemoteObjectUtil.loadObject(packedRemoteObject);
            CloseableIterator iterator = dataInterface.valueIterator(filter);
            writeValuesInBatches(iterator);
            iterator.close();
        }

        private void handleReadCachedValues() throws IOException {
            CloseableIterator<KeyValue> iterator = dataInterface.cachedValueIterator();
            writeKeyValuesInBatches(iterator);
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
                Log.e("Unexpected exception in request handler for data interface " + dataInterface.getName(), ex);
            } else {
                Log.e("Unexpected exception in request handler", ex);
            }
        }

        public DataInterface getDataInterface() {
            return dataInterface;
        }

        public long getStartTime() {
            return startTime;
        }

        private void handleIteratorWithKeyIterator() throws IOException {
            CloseableIterator<KeyValue> valueIt = dataInterface.iterator(IterableUtils.iterator(new SimpleIterator<Long>() {
                @Override
                public Long next() throws Exception {
                    return connection.readLong();
                }
            }, LONG_END));
            writeKeyValuesInBatches(valueIt);
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
                            Object value = readValue();
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

        private void handleIterator() throws IOException {
            CloseableIterator<KeyValue> iterator = dataInterface.iterator();
            writeKeyValuesInBatches(iterator);
            iterator.close();
        }

        private void writeValuesInBatches(CloseableIterator iterator) throws IOException {
            List<Object> currentBatchValues = new ArrayList<>();
            long batchSize = getBatchSize();
            while (iterator.hasNext()) {
                currentBatchValues.add(iterator.next());
                if (currentBatchValues.size() >= batchSize || memoryManager.getMemoryStatus() != MemoryStatus.FREE) {
                    writeValuesInBatch(currentBatchValues);
                    currentBatchValues.clear();
                }
            }
            if (!currentBatchValues.isEmpty()) {
                writeValuesInBatch(currentBatchValues);
            }
            connection.writeLong(LONG_END);
            connection.flush();
        }

        private void writeValuesInBatch(List<Object> currentBatchValues) throws IOException {
            connection.writeLong(currentBatchValues.size());
            writeListOfValues(currentBatchValues);
        }

        private void writeKeyValuesInBatches(CloseableIterator<KeyValue> iterator) throws IOException {
            //will write data in batches so we can compress key's and values separately
            List<Long> currentBatchKeys = new ArrayList<>();
            List<Object> currentBatchValues = new ArrayList<>();
            long batchSize = getBatchSize();
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

        private long getBatchSize() {
            int widthOfObject = dataInterface.getObjectSerializer().getObjectSize();
            return widthOfObject != -1 && widthOfObject < 16 ? CLONE_BATCH_SIZE_PRIMITIVE : CLONE_BATCH_SIZE_NON_PRIMITIVE;
        }

        private void writeCurrentBatch(List<Long> currentBatchKeys, List<Object> currentBatchValues) throws IOException {
            connection.writeLong(currentBatchKeys.size());
            writeListOfKeys(currentBatchKeys);
            writeListOfValues(currentBatchValues);
        }

        private void writeListOfValues(List<Object> currentBatchValues) throws IOException {
            //write values
            DataStream ds = new DataStream();
            ObjectSerializer objectSerializer = dataInterface.getObjectSerializer();
            for (Object value : currentBatchValues) {
                DataStreamUtils.writeValue(value, ds, objectSerializer);
            }
            byte[] origValues = ds.getNonEmptyBytes();
            byte[] compressedValues = Snappy.compress(origValues);
            //            Log.i("Compressed values from " + origValues.length + " to " + compressedValues.length);
            connection.writeByteArray(compressedValues);
        }

        private void writeListOfKeys(List<Long> currentBatchKeys) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (Long key : currentBatchKeys) {
                dos.writeLong(key);
            }
            dos.close();
            byte[] origKeys = bos.toByteArray();
            connection.writeByteArray(origKeys);
        }

        private void handleReadValue() throws IOException {
            long key = connection.readLong();
            Object value = dataInterface.read(key);
            DataStream ds = new DataStream();
            ObjectSerializer objectSerializer = dataInterface.getObjectSerializer();
            objectSerializer.writeValue(value, ds);
            int objectSize = objectSerializer.getObjectSize();
            if (objectSize == -1) {
                connection.writeInt(ds.position);
            }
            connection.writeByteArray(ds.buffer, ds.position);
        }

        private void handleMightContain() throws IOException {
            long key = connection.readLong();
            boolean mightContain = dataInterface.mightContain(key);
            connection.writeBoolean(mightContain);
        }

        private void handleWriteValue() throws IOException {
            long key = connection.readLong();
            Object value = readValue();
            dataInterface.write(key, value);
            connection.writeLong(LONG_OK);
        }

        private Object readValue() throws IOException {
            ObjectSerializer objectSerializer = dataInterface.getObjectSerializer();
            int objectSize = objectSerializer.getObjectSize();
            if (objectSize == -1) {
                objectSize = connection.readInt();
            }
            byte[] bytes = connection.readByteArray(objectSize);
            DataStream ds = new DataStream(bytes);
            return objectSerializer.readValue(ds, objectSize);
        }

        private Class readClass() throws IOException, ClassNotFoundException {
            String className = connection.readString();
            return Class.forName(className);
        }

    }

    public enum Action {
        READ_VALUE, WRITE_VALUE, ITERATOR_WITH_KEY_ITERATOR, READ_KEYS, WRITE_VALUES, DROP_ALL_DATA, CLOSE_CONNECTION, FLUSH,
        ITERATOR, READ_CACHED_VALUES, APPROXIMATE_SIZE, MIGHT_CONTAIN, EXACT_SIZE, OPTMIZE_FOR_READING,
        VALUES_ITERATOR_WITH_KEY_FILTER, ITERATOR_WITH_KEY_FILTER, VALUES_ITERATOR_WITH_VALUE_FILTER, ITERATOR_WITH_VALUE_FILTER
    }

    public enum ConnectionType {
        CONNECT_TO_INTERFACE, BATCH_WRITE_TO_INTERFACE, BATCH_READ_FROM_INTERFACE
    }

}
