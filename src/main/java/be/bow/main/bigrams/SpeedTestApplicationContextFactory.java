package be.bow.main.bigrams;

import be.bow.application.BaseApplicationContextFactory;
import be.bow.application.EnvironmentProperties;
import be.bow.application.MainClass;
import be.bow.web.WebContainerConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
* Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
*/
public class SpeedTestApplicationContextFactory extends BaseApplicationContextFactory {

    public SpeedTestApplicationContextFactory(Class<? extends MainClass> mainClass) {
        super(mainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        scan("be.bow");
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
