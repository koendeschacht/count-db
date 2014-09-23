package be.bagofwords.main.tests;

import be.bagofwords.application.BaseApplicationContextFactory;
import be.bagofwords.application.BaseRunnableApplicationContextFactory;
import be.bagofwords.application.EnvironmentProperties;
import be.bagofwords.application.MainClass;
import be.bagofwords.web.WebContainerConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class TestsApplicationContextFactory<T extends MainClass> extends BaseRunnableApplicationContextFactory {

    public TestsApplicationContextFactory(T mainClass) {
        super(mainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        scan("be.bagofwords");
        singleton("environmentProperties", new EnvironmentProperties() {
            @Override
            public boolean saveThreadSamplesToFile() {
                return true;
            }

            @Override
            public String getThreadSampleLocation() {
                return "./perf";
            }
        });
        bean(WebContainerConfiguration.class);
        return super.createApplicationContext();
    }

}
