package be.bow.db.application.environment;

import be.bow.application.EnvironmentProperties;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */
public interface RemoteCountDBEnvironmentProperties extends EnvironmentProperties {

    public String getDatabaseServerAddress();

    public int getDataInterfaceServerPort();

    public int getVirtualFileServerPort();

}
