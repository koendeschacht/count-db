package be.bagofwords.main.tests;

import be.bagofwords.application.BaseApplicationContextFactory;
import be.bagofwords.application.MainClass;
import be.bagofwords.application.status.RemoteRegisterUrlsServerProperties;
import be.bagofwords.web.WebContainer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class TestsApplicationContextFactory extends BaseApplicationContextFactory {

    public TestsApplicationContextFactory(MainClass mainClass) {
        super(mainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        scan("be.bagofwords");
        singleton("environmentProperties", new RemoteRegisterUrlsServerProperties() {
            @Override
            public boolean saveThreadSamplesToFile() {
                return true;
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
            public String getDatabaseServerAddress() {
                return "localhost";
            }

            @Override
            public int getRegisterUrlServerPort() {
                return 1210;
            }
        });
        singleton("webContainer", new WebContainer(getApplicationName()));
        return super.createApplicationContext();
    }

}
