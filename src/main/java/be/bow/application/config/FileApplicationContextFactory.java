package be.bow.application.config;

import be.bow.db.application.CountDBApplicationContextFactory;
import be.bow.main.DatabaseServerMain;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class FileApplicationContextFactory extends CountDBApplicationContextFactory {

    public FileApplicationContextFactory(Class<DatabaseServerMain> databaseServerMainClass) {
        super(databaseServerMainClass);
    }

    @Override
    public AnnotationConfigApplicationContext createApplicationContext() {
        bean(FileDataInterfaceConfiguration.class);
        return super.createApplicationContext();
    }
}
