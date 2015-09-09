package be.bagofwords.db.helper;


import be.bagofwords.application.ApplicationContextFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.context.ContextLoader;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/4/14.
 */

public class UnitTestContextLoader implements ContextLoader {

    @Override
    public String[] processLocations(Class<?> aClass, String... locations) {
        return locations;
    }

    @Override
    public ApplicationContext loadContext(String... locations) throws Exception {
        ApplicationContextFactory applicationContextFactory = new CountDBUnitTestApplicationContextFactory();
        applicationContextFactory.wireApplicationContext();
        AnnotationConfigApplicationContext applicationContext = applicationContextFactory.getApplicationContext();
        applicationContext.refresh();
        return applicationContext;
    }
}
