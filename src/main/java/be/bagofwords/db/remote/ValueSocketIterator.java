package be.bagofwords.db.remote;

import be.bagofwords.db.methods.DataStream;
import be.bagofwords.db.methods.DataStreamUtils;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.iterator.CloseableIterator;
import org.xerial.snappy.Snappy;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static be.bagofwords.db.remote.Protocol.LONG_END;
import static be.bagofwords.db.remote.Protocol.LONG_ERROR;

/**
 * Created by koen on 21/05/17.
 */
public class ValueSocketIterator<T> extends CloseableIterator<T> {

    private RemoteDataInterface<T> remoteDataInterface;
    private final Connection connection;
    private Iterator<T> nextValues;
    private boolean readAllValuesFromConnection;
    private final ObjectSerializer<T> objectSerializer;

    public ValueSocketIterator(RemoteDataInterface<T> remoteDataInterface, Connection connection) {
        this.remoteDataInterface = remoteDataInterface;
        this.connection = connection;
        this.readAllValuesFromConnection = false;
        this.objectSerializer = remoteDataInterface.getObjectSerializer();
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
                    byte[] compressedValues = connection.readByteArray();
                    byte[] uncompressedValues = Snappy.uncompress(compressedValues);
                    DataStream valueIS = new DataStream(uncompressedValues);
                    List<T> nextValuesList = new ArrayList<>();
                    while (nextValuesList.size() < numOfValues) {
                        int objectSize = DataStreamUtils.getObjectSize(valueIS, objectSerializer);
                        T value = objectSerializer.readValue(valueIS, objectSize);
                        nextValuesList.add(value);
                    }
                    if (nextValuesList.isEmpty()) {
                        throw new RuntimeException("Received zero values! numOfValues=" + numOfValues);
                    }
                    nextValues = nextValuesList.iterator();
                } else {
                    throw new RuntimeException("Unexpected response " + connection.readString());

                }
            } catch (Exception e) {
                remoteDataInterface.dropConnection(connection);
                throw new RuntimeException(e);
            }
        } else {
            nextValues = null;
        }
    }

    @Override
    public void closeInt() {
        if (readAllValuesFromConnection) {
            remoteDataInterface.releaseConnection(connection);
        } else {
            //server will still be sending data through this connection, so it can not be reused.
            remoteDataInterface.dropConnection(connection);
        }
    }

    @Override
    public boolean hasNext() {
        return nextValues != null;
    }

    @Override
    public T next() {
        T result = nextValues.next();
        if (!nextValues.hasNext()) {
            findNextValues();
        }
        return result;
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not implemented");
    }

}
