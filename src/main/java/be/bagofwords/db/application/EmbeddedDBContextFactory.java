package be.bagofwords.db.application;

import be.bagofwords.application.BaseApplicationContextFactory;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.virtualfile.local.LocalFileService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class EmbeddedDBContextFactory extends BaseApplicationContextFactory {

    private String dataDirectory;

    public EmbeddedDBContextFactory(String dataDirectory) {
        this.dataDirectory = dataDirectory;
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        scan("be.bagofwords");
        singleton("environmentProperties", new FileCountDBEnvironmentProperties() {
            @Override
            public boolean saveThreadSamplesToFile() {
                return false;
            }

            @Override
            public String getThreadSampleLocation() {
                return "./perf";
            }

            @Override
            public String getApplicationUrlRoot() {
                return "localhost";
            }

            @Override
            public String getDataDirectory() {
                return dataDirectory;
            }
        });
        bean(FileDataInterfaceFactory.class);
        bean(LocalFileService.class);
        return super.createApplicationContext();
    }

}
