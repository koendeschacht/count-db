package be.bow.application.environment;

import be.bow.application.EnvironmentProperties;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */
public interface CountDBEnvironmentProperties extends EnvironmentProperties {

    public String getDataDirectory();

    public String getDatabaseServerAddress();

}
