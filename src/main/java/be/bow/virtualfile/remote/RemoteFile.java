package be.bow.virtualfile.remote;

import be.bow.application.BaseServer;
import be.bow.util.WrappedSocketConnection;
import be.bow.virtualfile.VirtualFile;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RemoteFile implements VirtualFile {

    private String host;
    private int port;
    private File relPath;

    public RemoteFile(String host, int port, File relPath) {
        this.host = host;
        this.port = port;
        this.relPath = relPath;
    }

    @Override
    public VirtualFile getFile(String relativePath) {
        File newRelPath = new File(relPath, relativePath);
        return new RemoteFile(host, port, newRelPath);
    }

    @Override
    public InputStream createInputStream() {
        try {
            WrappedSocketConnection connection = new WrappedSocketConnection(host, port);
            connection.writeByte((byte) RemoteFileServer.Action.INPUT_STREAM.ordinal());
            connection.writeString(relPath.getPath());
            connection.flush();
            long answer = connection.readLong();
            if (answer == BaseServer.LONG_OK) {
                return connection.getIs();
            } else {
                String message = connection.readString();
                throw new RuntimeException("Received unexpected response while creating input stream to " + host + ":" + port + " " + message);
            }
        } catch (IOException exp) {
            throw new RuntimeException("Received exception while creating input stream to " + host + ":" + port, exp);
        }
    }

    @Override
    public OutputStream createOutputStream() {
        try {
            WrappedSocketConnection connection = new WrappedSocketConnection(host, 1220);
            connection.writeByte((byte) RemoteFileServer.Action.OUTPUT_STREAM.ordinal());
            connection.writeString(relPath.getPath());
            connection.flush();
            long answer = connection.readLong();
            if (answer == BaseServer.LONG_OK) {
                return connection.getOs();
            } else {
                String message = connection.readString();
                throw new RuntimeException("Received unexpected response while creating output stream to " + host + ":" + port + " " + message);
            }
        } catch (IOException exp) {
            throw new RuntimeException("Received exception while creating output stream to " + host + ":" + port, exp);
        }
    }

    @Override
    public boolean exists() {
        WrappedSocketConnection connection = null;
        try {
            connection = new WrappedSocketConnection(host, 1220);
            connection.writeByte((byte) RemoteFileServer.Action.EXISTS.ordinal());
            connection.writeString(relPath.getPath());
            connection.flush();
            return connection.readBoolean();
        } catch (IOException exp) {
            throw new RuntimeException("Received exception while querying exists to " + host + ":" + port, exp);
        } finally {
            IOUtils.closeQuietly(connection);
        }
    }
}
