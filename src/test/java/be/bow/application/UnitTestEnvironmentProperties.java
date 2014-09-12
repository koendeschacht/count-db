package be.bow.application;

import be.bow.application.environment.OnionDBEnvironmentProperties;

public class UnitTestEnvironmentProperties implements OnionDBEnvironmentProperties {

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
}
