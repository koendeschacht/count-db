package be.bagofwords.virtualfile.remote;

import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.virtualfile.VirtualFile;
import be.bagofwords.virtualfile.VirtualFileService;

import java.io.File;

public class RemoteFileService extends VirtualFileService {

    private String host;
    private int port;

    public RemoteFileService(ApplicationContext context) {
        this.host = context.getProperty("remote_file_service_host", "localhost");
        this.port = Integer.parseInt(context.getProperty("remote_file_server_port", "1208"));
    }

    @Override
    public VirtualFile getRootDirectory() {
        return new RemoteFile(host, port, new File("./"));
    }


}