package be.bagofwords.main;

import be.bagofwords.application.ApplicationManager;
import be.bagofwords.application.BaseRunnableApplicationContextFactory;
import be.bagofwords.application.MainClass;
import be.bagofwords.db.application.config.FileDataInterfaceConfiguration;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.db.remote.RemoteDataInterfaceServer;
import be.bagofwords.ui.UI;
import be.bagofwords.virtualfile.remote.RemoteFileServer;
import be.bagofwords.web.WebContainerConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;

public class DatabaseServerMain implements MainClass {

    @Autowired
    private RemoteDataInterfaceServer remoteDataInterfaceServer;
    @Autowired
    private RemoteFileServer remoteFileServer;

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            UI.writeError("Expected exactly 3 arguments, the directory to store the data files (e.g. /home/some_user/data/), the port of the data interface server (e.g. 1208) and the port of the virtual file server (e.g. 1209)");
        } else {
            String dataDirectory = args[0];
            int dataInterfacePort = Integer.parseInt(args[1]);
            int virtualFileServerPort = Integer.parseInt(args[2]);
            ApplicationManager.runSafely(new DatabaseServerMainContextFactory(new DatabaseServerMain(), dataDirectory, dataInterfacePort, virtualFileServerPort));
        }
    }

    @Override
    public void run() {
        remoteDataInterfaceServer.start();
        remoteFileServer.start();
        remoteDataInterfaceServer.waitForFinish();
        remoteFileServer.waitForFinish();
    }

    public static class DatabaseServerMainEnvironmentProperties implements FileCountDBEnvironmentProperties, RemoteCountDBEnvironmentProperties {

        private String dataDirectory;
        private final int dataInterfacePort;
        private final int virtualFileServerPort;

        public DatabaseServerMainEnvironmentProperties(String dataDirectory, int dataInterfacePort, int virtualFileServerPort) {
            this.dataDirectory = dataDirectory;
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
        public int getDataInterfaceServerPort() {
            return dataInterfacePort;
        }

        @Override
        public int getVirtualFileServerPort() {
            return virtualFileServerPort;
        }
    }

    public static class DatabaseServerMainContextFactory extends BaseRunnableApplicationContextFactory {

        private String dataDirectory;
        private final int dataInterfacePort;
        private final int virtualFileServerPort;

        public DatabaseServerMainContextFactory(DatabaseServerMain databaseServerMainClass, String dataDirectory, int dataInterfacePort, int virtualFileServerPort) {
            super(databaseServerMainClass);
            this.dataDirectory = dataDirectory;
            this.dataInterfacePort = dataInterfacePort;
            this.virtualFileServerPort = virtualFileServerPort;
        }

        @Override
        public AnnotationConfigApplicationContext createApplicationContext() {
            scan("be.bagofwords");
            bean(WebContainerConfiguration.class);
            singleton("environmentProperties", new DatabaseServerMainEnvironmentProperties(dataDirectory, dataInterfacePort, virtualFileServerPort));
            bean(FileDataInterfaceConfiguration.class);
            return super.createApplicationContext();
        }
    }
}
