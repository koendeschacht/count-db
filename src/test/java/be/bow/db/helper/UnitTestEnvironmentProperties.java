package be.bow.db.helper;

import be.bow.db.application.environment.FileCountDBEnvironmentProperties;
import be.bow.db.application.environment.RemoteCountDBEnvironmentProperties;

public class UnitTestEnvironmentProperties implements FileCountDBEnvironmentProperties, RemoteCountDBEnvironmentProperties {

    public static final int DATA_INTERFACE_SERVER_PORT = 1234;
    public static final int VIRTUAL_FILE_SERVER_PORT = 1235;
    private String dataDirectory = "/tmp/unitTest/" + System.currentTimeMillis() + "/";

    @Override
    public boolean saveThreadSamplesToFile() {
        return false;
    }

    @Override
    public String getThreadSampleLocation() {
        return null; //should not be used
    }

    @Override
    public String getDataDirectory() {
        return dataDirectory;
    }

    @Override
    public String getDatabaseServerAddress() {
        return "localhost";
    }

    @Override
    public int getDataInterfaceServerPort() {
        return DATA_INTERFACE_SERVER_PORT;
    }

    @Override
    public int getVirtualFileServerPort() {
        return VIRTUAL_FILE_SERVER_PORT;
    }
}
