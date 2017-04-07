package be.bagofwords.main;

import be.bagofwords.application.SocketServer;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.remote.RemoteDataInterfaceServer;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.ApplicationManager;
import be.bagofwords.minidepi.annotations.Inject;
import be.bagofwords.ui.UI;
import be.bagofwords.virtualfile.local.LocalFileService;
import be.bagofwords.virtualfile.remote.RemoteFileServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseServerMain implements Runnable {

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            UI.writeError("Expected exactly 4 arguments, the directory to store the data files (e.g. /home/some_user/data/), the url of this server (e.g. www.myawesomeserver.com) the port of the data interface server (e.g. 1208) and the port of the virtual file server (e.g. 1209)");
        } else {
            Map<String, String> config = new HashMap<>();
            config.put("application_name", "database_server_main");
            config.put("data_directory", args[0]);
            config.put("server_url", args[1]);
            config.put("socket_port", args[2]);
            config.put("virtual_file_server_port", args[3]);
            ApplicationManager.run(new DatabaseServerMain(), config);
        }
    }

    @Inject
    private FileDataInterfaceFactory fileDataInterfaceFactory;
    @Inject
    private LocalFileService localFileService;
    @Inject
    private RemoteFileServer remoteFileServer;
    @Inject
    private RemoteDataInterfaceServer remoteDataInterfaceServer;
    @Inject
    private SocketServer socketServer;
    @Inject
    private ApplicationContext applicationContext;

    @Override
    public void run() {
        applicationContext.waitUntilTerminated();
    }

}
