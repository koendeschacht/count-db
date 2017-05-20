package be.bagofwords.db.application;

import be.bagofwords.application.MinimalApplicationDependencies;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.annotations.Inject;

import java.util.HashMap;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class EmbeddedDBContextFactory {

    public static DataInterfaceFactory createDataInterfaceFactory(String dataDirectory) {
        HashMap<String, String> config = new HashMap<>();
        config.put("data_directory", dataDirectory);
        ApplicationContext applicationContext = new ApplicationContext(config);
        applicationContext.registerBean(MinimalApplicationDependencies.class);
        return applicationContext.getBean(ApplicationContextEnclosingFileDataInterfaceFactory.class);
    }

    public static class ApplicationContextEnclosingFileDataInterfaceFactory extends FileDataInterfaceFactory {

        private ApplicationContext applicationContext;
        private boolean didTerminate = false;

        @Inject
        public ApplicationContextEnclosingFileDataInterfaceFactory(ApplicationContext context) {
            super(context);
            this.applicationContext = context;
        }

        @Override
        public synchronized void terminate() {
            if (!didTerminate) {
                didTerminate = true;
                super.terminate();
                applicationContext.terminate();
            }
        }
    }
}
