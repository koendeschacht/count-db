package be.bow.db.application;

import be.bow.application.BaseApplicationContextFactory;
import be.bow.application.MainClass;
import be.bow.web.WebContainerConfiguration;
import be.bow.application.environment.BaseOnionDBEnvironmentProperties;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class OnionDBApplicationContextFactory extends BaseApplicationContextFactory {

    public OnionDBApplicationContextFactory(Class<? extends MainClass> mainClass) {
        super(mainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        scan("be.bow");
        singleton("environmentProperties", new BaseOnionDBEnvironmentProperties());
        bean(WebContainerConfiguration.class);
        return super.createApplicationContext();
    }

}
