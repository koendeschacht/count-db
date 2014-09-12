package be.bow.main;

import be.bow.application.ApplicationManager;
import be.bow.application.BaseApplicationContextFactory;
import be.bow.application.MainClass;
import be.bow.db.application.config.FileDataInterfaceConfiguration;
import be.bow.db.application.environment.FileCountDBEnvironmentProperties;
import be.bow.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bow.db.remote.RemoteDataInterfaceServer;
import be.bow.ui.UI;
import be.bow.virtualfile.remote.RemoteFileServer;
import be.bow.web.WebContainerConfiguration;
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
            ApplicationManager.runSafely(new DatabaseServerMainContextFactory(DatabaseServerMain.class, dataDirectory, dataInterfacePort, virtualFileServerPort));
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

    public static class DatabaseServerMainContextFactory extends BaseApplicationContextFactory {

        private String dataDirectory;
        private final int dataInterfacePort;
        private final int virtualFileServerPort;

        public DatabaseServerMainContextFactory(Class<DatabaseServerMain> databaseServerMainClass, String dataDirectory, int dataInterfacePort, int virtualFileServerPort) {
            super(databaseServerMainClass);
            this.dataDirectory = dataDirectory;
            this.dataInterfacePort = dataInterfacePort;
            this.virtualFileServerPort = virtualFileServerPort;
        }

        @Override
        public AnnotationConfigApplicationContext createApplicationContext() {
            scan("be.bow");
            bean(WebContainerConfiguration.class);
            singleton("environmentProperties", new DatabaseServerMainEnvironmentProperties(dataDirectory, dataInterfacePort, virtualFileServerPort));
            bean(FileDataInterfaceConfiguration.class);
            return super.createApplicationContext();
        }
    }
}
