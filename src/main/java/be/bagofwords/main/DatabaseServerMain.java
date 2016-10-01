package be.bagofwords.main;

import be.bagofwords.application.ApplicationManager;
import be.bagofwords.application.BaseApplicationContextFactory;
import be.bagofwords.application.EnvironmentProperties;
import be.bagofwords.application.MainClass;
import be.bagofwords.application.status.ListUrlsController;
import be.bagofwords.application.status.RegisterUrlsClient;
import be.bagofwords.application.status.RemoteRegisterUrlsServerProperties;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.remote.RemoteDataInterfaceServer;
import be.bagofwords.ui.UI;
import be.bagofwords.virtualfile.local.LocalFileService;
import be.bagofwords.virtualfile.remote.RemoteFileServer;
import be.bagofwords.web.WebContainer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;

public class DatabaseServerMain implements MainClass {

    @Autowired
    private RemoteDataInterfaceServer remoteDataInterfaceServer;
    @Autowired
    private RemoteFileServer remoteFileServer;
    @Autowired
    private ListUrlsController listUrlsController;

    public static void main(String[] args) throws IOException {
        if (args.length != 4) {
            UI.writeError("Expected exactly 4 arguments, the directory to store the data files (e.g. /home/some_user/data/), the url of this server (e.g. www.myawesomeserver.com) the port of the data interface server (e.g. 1208) and the port of the virtual file server (e.g. 1209)");
        } else {
            String dataDirectory = args[0];
            String serverUrl = args[1];
            int dataInterfacePort = Integer.parseInt(args[2]);
            int virtualFileServerPort = Integer.parseInt(args[3]);
            ApplicationManager.runSafely(new DatabaseServerMainContextFactory(new DatabaseServerMain(), dataDirectory, serverUrl, dataInterfacePort, virtualFileServerPort));
        }
    }

    @Override
    public void run() {
        remoteDataInterfaceServer.start();
        remoteFileServer.start();
        remoteDataInterfaceServer.waitForFinish();
        remoteFileServer.waitForFinish();
    }

    public static class DatabaseServerMainEnvironmentProperties implements FileCountDBEnvironmentProperties, RemoteCountDBEnvironmentProperties, RemoteRegisterUrlsServerProperties {

        private final String dataDirectory;
        private final String serverUrl;
        private final int dataInterfacePort;
        private final int virtualFileServerPort;

        public DatabaseServerMainEnvironmentProperties(String dataDirectory, String serverUrl, int dataInterfacePort, int virtualFileServerPort) {
            this.dataDirectory = dataDirectory;
            this.serverUrl = serverUrl;
            this.dataInterfacePort = dataInterfacePort;
            this.virtualFileServerPort = virtualFileServerPort;
        }

        @Override
        public String getDataDirectory() {
            return dataDirectory;
        }

        @Override
        public boolean saveThreadSamplesToFile() {
            return true;
        }

        @Override
        public String getThreadSampleLocation() {
            return dataDirectory + "/perf";
        }

        @Override
        public String getDatabaseServerAddress() {
            return "localhost";
        }

        @Override
        public int getRegisterUrlServerPort() {
            return 1210;
        }

        @Override
        public int getDataInterfaceServerPort() {
            return dataInterfacePort;
        }

        @Override
        public int getVirtualFileServerPort() {
            return virtualFileServerPort;
        }

        @Override
        public String getApplicationUrlRoot() {
            return serverUrl;
        }
    }

    public static class DatabaseServerMainContextFactory extends BaseApplicationContextFactory {

        private final EnvironmentProperties environmentProperties;

        public DatabaseServerMainContextFactory(DatabaseServerMain databaseServerMainClass, String dataDirectory, String serverUrl, int dataInterfacePort, int virtualFileServerPort) {
            super(databaseServerMainClass);
            this.environmentProperties = new DatabaseServerMainEnvironmentProperties(dataDirectory, serverUrl, dataInterfacePort, virtualFileServerPort);
        }

        @Override
        public ApplicationContext wireApplicationContext() {
            scan("be.bagofwords");
            singleton("webContainer", new WebContainer(getApplicationName()));
            singleton("environmentProperties", environmentProperties);
            bean(FileDataInterfaceFactory.class);
            bean(LocalFileService.class);
            //Register urls with central url list
            bean(RegisterUrlsClient.class);
            return super.wireApplicationContext();
        }
    }
}
