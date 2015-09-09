package be.bagofwords.db.helper;

import be.bagofwords.util.UnitTestApplicationContextFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class CountDBUnitTestApplicationContextFactory extends UnitTestApplicationContextFactory {

    @Override
    public void wireApplicationContext() {
        getApplicationContext().getBeanFactory().registerSingleton("environmentProperties", new UnitTestEnvironmentProperties());
        super.wireApplicationContext();
    }
}
