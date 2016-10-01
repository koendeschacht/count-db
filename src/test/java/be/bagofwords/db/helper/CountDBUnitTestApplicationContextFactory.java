package be.bagofwords.db.helper;

import be.bagofwords.util.UnitTestApplicationContextFactory;
import org.springframework.context.ApplicationContext;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/5/14.
 */
public class CountDBUnitTestApplicationContextFactory extends UnitTestApplicationContextFactory {

    @Override
    public ApplicationContext wireApplicationContext() {
        getApplicationContext().getBeanFactory().registerSingleton("environmentProperties", new UnitTestEnvironmentProperties());
        return super.wireApplicationContext();
    }
}
