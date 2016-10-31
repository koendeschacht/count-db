package be.bagofwords.main.tests;

import be.bagofwords.application.ApplicationContext;
import be.bagofwords.application.BaseApplicationContextFactory;

import java.util.Map;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class TestsApplicationContextFactory extends BaseApplicationContextFactory {

    @Override
    public ApplicationContext createApplicationContext(Map<String, String> config) {
        return super.createApplicationContext(config);
    }
}
