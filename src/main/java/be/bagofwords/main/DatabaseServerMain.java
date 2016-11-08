package be.bagofwords.main;

import be.bagofwords.application.*;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.remote.RemoteDataInterfaceServer;
import be.bagofwords.ui.UI;
import be.bagofwords.virtualfile.local.LocalFileService;
import be.bagofwords.virtualfile.remote.RemoteFileServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DatabaseServerMain implements MainClass {


    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            UI.writeError("Expected exactly 4 arguments, the directory to store the data files (e.g. /home/some_user/data/), the url of this server (e.g. www.myawesomeserver.com) the port of the data interface server (e.g. 1208) and the port of the virtual file server (e.g. 1209)");
        } else {
            Map<String, String> config = new HashMap<>();
            config.put("application_name", "database_server_main");
            config.put("data_directory", args[0]);
            config.put("server_url", args[1]);
            config.put("remote_interface_port", args[2]);
            config.put("virtual_file_server_port", args[3]);
            ApplicationManager.runSafely(new DatabaseServerMain(), config, new DatabaseServerContextFactory());
        }
    }

    private static class DatabaseServerContextFactory extends MinimalApplicationContextFactory {
        @Override
        public void wireApplicationContext(ApplicationContext context) {
            super.wireApplicationContext(context);
            context.registerBean(new FileDataInterfaceFactory(context));
            context.registerBean(new LocalFileService(context));
            context.registerBean(new RemoteFileServer(context));
            context.registerBean(new RemoteDataInterfaceServer(context));
        }
    }

    @Override
    public void run(ApplicationContext context) {
        SocketServer socketServer = context.getBean(SocketServer.class);
        socketServer.start();
        socketServer.waitForFinish();
    }


}
