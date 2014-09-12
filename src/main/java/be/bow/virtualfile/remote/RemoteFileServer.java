package be.bow.virtualfile.remote;

import be.bow.application.BaseServer;
import be.bow.application.annotations.BowComponent;
import be.bow.ui.UI;
import be.bow.util.WrappedSocketConnection;
import be.bow.application.environment.OnionDBEnvironmentProperties;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;

@BowComponent
public class RemoteFileServer extends BaseServer {

    private File rootDirectory;
    private ServerSocket serverSocket;

    @Autowired
    private void setRootDirectory(OnionDBEnvironmentProperties environmentProperties) {
        File dataDir = new File(environmentProperties.getDataDirectory());
        rootDirectory = new File(dataDir, "virtualFiles");
        try {
            FileUtils.forceMkdir(rootDirectory);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory " + rootDirectory.getAbsolutePath(), e);
        }
    }

    public RemoteFileServer() {
        super("RemoteFileServer", 1220);
    }

    @Override
    protected BaseServer.SocketRequestHandler createSocketRequestHandler(WrappedSocketConnection connection) throws IOException {
        return new SocketRequestHandler(connection);
    }


    private class SocketRequestHandler extends BaseServer.SocketRequestHandler {

        private long totalNumberOfRequests = 0;

        public SocketRequestHandler(WrappedSocketConnection connection) throws IOException {
            super(connection);
        }

        @Override
        protected void reportUnexpectedError(Exception ex) {
            UI.writeError("Exception in socket request handler of remote file server", ex);
        }

        @Override
        public long getTotalNumberOfRequests() {
            return totalNumberOfRequests;
        }

        @Override
        protected void handleRequests() throws Exception {
            //We only handle a single request:
            byte actionAsByte = connection.readByte();
            Action action = Action.values()[actionAsByte];
            if (action == Action.INPUT_STREAM) {
                String relPath = connection.readString();
                File file = new File(rootDirectory, relPath);
                if (file.exists()) {
                    connection.writeLong(LONG_OK);
                    FileInputStream fis = new FileInputStream(file);
                    IOUtils.copy(fis, connection.getOs());
                    connection.flush();
                } else {
                    connection.writeLong(LONG_ERROR);
                    connection.writeString("Could not find file " + file.getAbsolutePath());
                    connection.flush();
                }
            } else if (action == Action.OUTPUT_STREAM) {
                String relPath = connection.readString();
                File file = new File(rootDirectory, relPath);
                if (!file.exists()) {
                    FileUtils.forceMkdir(file.getParentFile());
                }
                if (file.isDirectory()) {
                    connection.writeLong(LONG_ERROR);
                    connection.writeString("File " + file.getAbsolutePath() + " is a directory!");
                } else {
                    connection.writeLong(LONG_OK);
                    connection.flush();
                    FileOutputStream fos = new FileOutputStream(file);
                    IOUtils.copy(connection.getIs(), fos);
                }
            } else if (action == Action.EXISTS) {
                String relPath = connection.readString();
                File file = new File(rootDirectory, relPath);
                connection.writeBoolean(file.exists());
                connection.flush();
            } else {
                connection.writeLong(LONG_ERROR);
                connection.writeString("Unknown command " + action);
            }
        }
    }

    public static enum Action {
        INPUT_STREAM, OUTPUT_STREAM, EXISTS
    }
}
