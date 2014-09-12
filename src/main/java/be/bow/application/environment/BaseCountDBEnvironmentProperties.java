package be.bow.application.environment;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */
public class BaseCountDBEnvironmentProperties implements CountDBEnvironmentProperties {

    public String getDataDirectory() {
        return "/home/koen/bow/data/";
    }

    @Override
    public String getDatabaseServerAddress() {
        return "localhost";
    }

    public boolean saveThreadSamplesToFile() {
        return true;
    }

    public String getThreadSampleLocation() {
        return "/home/koen/perf";
    }

}
