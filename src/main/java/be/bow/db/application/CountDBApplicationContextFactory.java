package be.bow.db.application;

import be.bow.application.BaseApplicationContextFactory;
import be.bow.application.MainClass;
import be.bow.application.environment.BaseCountDBEnvironmentProperties;
import be.bow.web.WebContainerConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class CountDBApplicationContextFactory extends BaseApplicationContextFactory {

    public CountDBApplicationContextFactory(Class<? extends MainClass> mainClass) {
        super(mainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        scan("be.bow");
        singleton("environmentProperties", new BaseCountDBEnvironmentProperties());
        bean(WebContainerConfiguration.class);
        return super.createApplicationContext();
    }

}
