package be.bow.main;

import be.bow.application.ApplicationManager;
import be.bow.application.MainClass;
import be.bow.application.config.LocalDBOnionDBApplicationContextFactory;
import be.bow.db.remote.RemoteDataInterfaceServer;
import be.bow.virtualfile.remote.RemoteFileServer;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

public class DatabaseServerMain implements MainClass {

    @Autowired
    private RemoteDataInterfaceServer remoteDataInterfaceServer;
    @Autowired
    private RemoteFileServer remoteFileServer;

    public static void main(String[] args) throws IOException {
        ApplicationManager.runSafely(new LocalDBOnionDBApplicationContextFactory(DatabaseServerMain.class));
    }

    @Override
    public void run() {
        remoteDataInterfaceServer.start();
        remoteFileServer.start();
        remoteDataInterfaceServer.waitForFinish();
        remoteFileServer.waitForFinish();
    }
}
