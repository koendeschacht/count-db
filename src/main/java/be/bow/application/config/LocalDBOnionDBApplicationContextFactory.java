package be.bow.application.config;

import be.bow.db.application.OnionDBApplicationContextFactory;
import be.bow.main.DatabaseServerMain;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class LocalDBOnionDBApplicationContextFactory extends OnionDBApplicationContextFactory {

    public LocalDBOnionDBApplicationContextFactory(Class<DatabaseServerMain> databaseServerMainClass) {
        super(databaseServerMainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        bean(LocalDataInterfaceConfiguration.class);
        return super.createApplicationContext();
    }
}
