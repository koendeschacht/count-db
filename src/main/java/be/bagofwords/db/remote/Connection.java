package be.bagofwords.db.remote;

import be.bagofwords.util.SocketConnection;

import java.io.IOException;

import static be.bagofwords.db.remote.Protocol.LONG_ERROR;

/**
 * Created by koen on 21/05/17.
 */
public class Connection extends SocketConnection {

    private RemoteDataInterface remoteDataInterface;
    private boolean isTaken;
    private long lastUsage;

    public Connection(RemoteDataInterface remoteDataInterface, String host, int port, boolean useLargeOutputBuffer, boolean useLargeInputBuffer, RemoteDataInterfaceServer.ConnectionType connectionType) throws IOException {
        super(host, port, false, false);
        this.remoteDataInterface = remoteDataInterface;
        if (useLargeOutputBuffer) {
            useLargeOutputBuffer();
        }
        if (useLargeInputBuffer) {
            useLargeInputBuffer();
        }
        initializeSubset(connectionType);
    }

    private void initializeSubset(RemoteDataInterfaceServer.ConnectionType connectionType) throws IOException {
        writeString(RemoteDataInterfaceServer.NAME);
        writeByte((byte) connectionType.ordinal());
        writeString(remoteDataInterface.getName());
        writeBoolean(remoteDataInterface.isTemporaryDataInterface());
        writeString(remoteDataInterface.getObjectClass().getCanonicalName());
        writeString(remoteDataInterface.getCombinator().getClass().getCanonicalName());
        flush();
        long response = readLong();
        if (response == LONG_ERROR) {
            String errorMessage = readString();
            throw new RuntimeException("Received unexpected message while initializing interface " + errorMessage);
        }
    }

    public boolean isTaken() {
        return isTaken;
    }

    public void setTaken(boolean taken) {
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
        remoteDataInterface.doAction(RemoteDataInterfaceServer.Action.CLOSE_CONNECTION, this);
        super.close();
    }
}
