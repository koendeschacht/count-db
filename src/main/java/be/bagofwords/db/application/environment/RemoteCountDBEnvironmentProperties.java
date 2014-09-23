package be.bagofwords.db.application.environment;

import be.bagofwords.application.EnvironmentProperties;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */
public interface RemoteCountDBEnvironmentProperties extends EnvironmentProperties {

    public String getDatabaseServerAddress();

    public int getDataInterfaceServerPort();

    public int getVirtualFileServerPort();

}
