package be.bagofwords.db.remote;

import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.SerializationUtils;
import org.xerial.snappy.Snappy;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
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
    private final int objectLength;

    public ValueSocketIterator(RemoteDataInterface<T> remoteDataInterface, Connection connection) {
        this.remoteDataInterface = remoteDataInterface;
        this.connection = connection;
        this.readAllValuesFromConnection = false;
        this.objectLength = SerializationUtils.getWidth(remoteDataInterface.getObjectClass());
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
                    DataInputStream valueIS = new DataInputStream(new ByteArrayInputStream(uncompressedValues));
                    List<T> nextValuesList = new ArrayList<>();
                    while (nextValuesList.size() < numOfValues) {
                        int length;
                        if (objectLength == -1) {
                            length = valueIS.readInt();
                        } else {
                            length = objectLength;
                        }
                        byte[] objectAsBytes = new byte[length];
                        if (length > 0) {
                            int bytesRead = valueIS.read(objectAsBytes);
                            if (bytesRead < length) {
                                throw new RuntimeException("Read " + bytesRead + " bytes, expected " + length);
                            }
                        }
                        T value = SerializationUtils.bytesToObjectCheckForNull(objectAsBytes, remoteDataInterface.getObjectClass());
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
