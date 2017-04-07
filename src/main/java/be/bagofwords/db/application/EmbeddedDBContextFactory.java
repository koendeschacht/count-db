package be.bagofwords.db.application;

import be.bagofwords.application.MinimalApplicationDependencies;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.minidepi.ApplicationContext;

import java.util.HashMap;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class EmbeddedDBContextFactory {

    public static DataInterfaceFactory createDataInterfaceFactory(String dataDirectory) {
        HashMap<String, String> config = new HashMap<>();
        config.put("data_directory", dataDirectory);
        ApplicationContext applicationContext = new ApplicationContext();
        applicationContext.registerBean(MinimalApplicationDependencies.class);
        return applicationContext.getBean(DataInterfaceFactory.class);
    }
}
