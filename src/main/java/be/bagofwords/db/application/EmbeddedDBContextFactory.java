package be.bagofwords.db.application;

import be.bagofwords.application.ApplicationContext;
import be.bagofwords.application.MinimalApplicationContextFactory;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.virtualfile.local.LocalFileService;

import java.util.HashMap;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class EmbeddedDBContextFactory extends MinimalApplicationContextFactory {

    @Override
    public void wireApplicationContext(ApplicationContext context) {
        super.wireApplicationContext(context);
        context.registerBean(new FileDataInterfaceFactory(context));
        context.registerBean(new LocalFileService(context));
    }

    public static DataInterfaceFactory createDataInterfaceFactory(String dataDirectory) {
        HashMap<String, String> config = new HashMap<>();
        config.put("data_directory", dataDirectory);
        ApplicationContext applicationContext = new EmbeddedDBContextFactory().createApplicationContext(config);
        return applicationContext.getBean(DataInterfaceFactory.class);
    }
}
