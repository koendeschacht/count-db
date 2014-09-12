package be.bow.db.helper;


import be.bow.application.ApplicationContextFactory;
import be.bow.db.helper.CountDBUnitTestApplicationContextFactory;
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
        AnnotationConfigApplicationContext applicationContext = applicationContextFactory.createApplicationContext();
        applicationContext.refresh();
        return applicationContext;
    }
}
