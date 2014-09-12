package be.bow.main.test;

import be.bow.application.ApplicationManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseCachingType;
import be.bow.db.LongCombinator;
import be.bow.db.application.OnionDBApplicationContextFactory;

import java.io.IOException;

public class TestDatabaseSpeedCounts extends TestDatabaseSpeed {

    public static void main(String[] args) throws IOException, InterruptedException {
        ApplicationManager.runSafely(new OnionDBApplicationContextFactory(TestDatabaseSpeedCounts.class));
    }

    @Override
    protected DataInterface createDataInterface(DatabaseCachingType type, DataInterfaceFactory factory) {
        return factory.createDataInterface(type, "testCounts_" + type, Long.class, new LongCombinator());
    }

    @Override
    protected void doRead(String prev, String word, DataInterface dataInterface) {
        if (prev != null) {
            dataInterface.readCount(prev + " " + word);
        }
    }

    @Override
    protected void doWrite(String prev, String word, DataInterface dataInterface) {
        if (prev != null) {
            dataInterface.increaseCount(prev + " " + word);
        }
    }
}
