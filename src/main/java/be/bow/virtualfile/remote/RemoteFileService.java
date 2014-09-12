package be.bow.virtualfile.remote;

import be.bow.virtualfile.VirtualFile;
import be.bow.virtualfile.VirtualFileService;

import java.io.File;

public class RemoteFileService extends VirtualFileService {

    private String host;

    public RemoteFileService(String remoteServerAddress) {
        this.host = remoteServerAddress;
    }

    @Override
    public VirtualFile getRootDirectory() {
        return new RemoteFile(host, new File("./"));
    }


}