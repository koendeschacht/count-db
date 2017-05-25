package be.bagofwords.virtualfile.remote;

import be.bagofwords.logging.Log;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.util.SocketConnection;
import be.bagofwords.virtualfile.VirtualFile;
import be.bagofwords.virtualfile.VirtualFileService;
import be.bagofwords.web.SocketRequestHandler;
import be.bagofwords.web.SocketRequestHandlerFactory;
import be.bagofwords.web.SocketServer;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static be.bagofwords.db.remote.Protocol.LONG_ERROR;
import static be.bagofwords.db.remote.Protocol.LONG_OK;

public class RemoteFileServer implements SocketRequestHandlerFactory {

    public static final String NAME = "RemoteFileServer";

    private VirtualFileService virtualFileService;

    public RemoteFileServer(ApplicationContext applicationContext) {
        this.virtualFileService = applicationContext.getBean(VirtualFileService.class);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public SocketRequestHandler createSocketRequestHandler(SocketConnection socketConnection) throws IOException {
        return new RemoteFileSocketRequestHandler(socketConnection);
    }

    private class RemoteFileSocketRequestHandler extends SocketRequestHandler {

        private long totalNumberOfRequests = 0;

        public RemoteFileSocketRequestHandler(SocketConnection connection) throws IOException {
            super(connection);
        }

        @Override
        public void reportUnexpectedError(Exception ex) {
            Log.e("Exception in socket request handler of remote file server", ex);
        }

        @Override
        public long getTotalNumberOfRequests() {
            return totalNumberOfRequests;
        }

        @Override
        public void handleRequests() throws Exception {
            //We only handle a single request:
            byte actionAsByte = connection.readByte();
            try {
                Action action = Action.values()[actionAsByte];
                if (action == Action.INPUT_STREAM) {
                    String relPath = connection.readString();
                    VirtualFile file = virtualFileService.getRootDirectory().getFile(relPath);
                    if (file.exists()) {
                        InputStream is = file.createInputStream();
                        connection.writeLong(LONG_OK);
                        IOUtils.copy(is, connection.getOs());
                        connection.flush();
                    } else {
                        connection.writeLong(LONG_ERROR);
                        connection.writeString("Could not find file " + relPath);
                        connection.flush();
                    }
                } else if (action == Action.OUTPUT_STREAM) {
                    String relPath = connection.readString();
                    VirtualFile file = virtualFileService.getRootDirectory().getFile(relPath);
                    OutputStream os = file.createOutputStream();
                    connection.writeLong(LONG_OK);
                    connection.flush();
                    IOUtils.copy(connection.getIs(), os);
                } else if (action == Action.EXISTS) {
                    String relPath = connection.readString();
                    VirtualFile file = virtualFileService.getRootDirectory().getFile(relPath);
                    connection.writeBoolean(file.exists());
                    connection.flush();
                } else {
                    connection.writeLong(LONG_ERROR);
                    connection.writeString("Unknown command " + action);
                    connection.flush();
                }
            } catch (Exception exp) {
                Log.e("Unexpected exception while trying to handle request in RemoteFileServer, will try to send error message to client.", exp);
                //try to send error message to client (only works if the client happens to be checking for LONG_ERROR)
                connection.writeLong(LONG_ERROR);
                connection.writeString(exp.getMessage());
                connection.flush();
                throw exp; //throw to caller method to close this connection
            }
        }
    }

    public enum Action {
        INPUT_STREAM, OUTPUT_STREAM, EXISTS
    }
}
