package be.bow.virtualfile.remote;

import be.bow.virtualfile.VirtualFile;
import be.bow.virtualfile.VirtualFileService;

import java.io.File;

public class RemoteFileService extends VirtualFileService {

    private String host;
    private int port;

    public RemoteFileService(String remoteServerAddress, int remoteServerPort) {
        this.host = remoteServerAddress;
        this.port = remoteServerPort;
    }

    @Override
    public VirtualFile getRootDirectory() {
        return new RemoteFile(host, port, new File("./"));
    }


}