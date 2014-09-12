package be.bow.main.test;

import be.bow.application.ApplicationManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseCachingType;
import be.bow.db.OverWriteCombinator;
import be.bow.db.application.OnionDBApplicationContextFactory;

import java.io.IOException;

public class TestDatabaseSpeedStrings extends TestDatabaseSpeed {

    public static void main(String[] args) throws IOException, InterruptedException {
        ApplicationManager.runSafely(new OnionDBApplicationContextFactory(TestDatabaseSpeedStrings.class));
    }


    @Override
    protected DataInterface createDataInterface(DatabaseCachingType type, DataInterfaceFactory factory) {
        return factory.createDataInterface(type, "testStrings_" + type, String.class, new OverWriteCombinator<String>());
    }

    @Override
    protected void doRead(String prev, String word, DataInterface dataInterface) {
        if (prev != null) {
            dataInterface.read(prev + " " + word);
        }
    }

    @Override
    protected void doWrite(String prev, String word, DataInterface dataInterface) {
        if (prev != null) {
            dataInterface.write(prev + " " + word, prev + " " + word);
        }
    }
}
