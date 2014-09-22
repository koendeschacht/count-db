package be.bow.main.tests.random;

import be.bow.db.DataInterface;
import be.bow.util.SafeThread;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
class RandomReadsWritesThread extends SafeThread {

    private final MutableLong numberOfItemsProcessed;
    private final MutableLong timeSpend;
    private final long numberOfItems;
    private final DataInterface dataInterface;
    private CountDownLatch countDownLatch;
    private final boolean writeValues;

    public RandomReadsWritesThread(MutableLong numberOfItemsProcessed, MutableLong timeSpend, long numberOfItems, DataInterface dataInterface, CountDownLatch countDownLatch, boolean writeValues) {
        super("ReadTextThread", false);
        this.numberOfItemsProcessed = numberOfItemsProcessed;
        this.timeSpend = timeSpend;
        this.numberOfItems = numberOfItems;
        this.dataInterface = dataInterface;
        this.countDownLatch = countDownLatch;
        this.writeValues = writeValues;
    }

    public void runInt() throws IOException {
        Random random = new Random();
        while (numberOfItemsProcessed.longValue() < numberOfItems) {
            long startTime = System.nanoTime();
            long numberOfItemsPerIteration = numberOfItems / 10000;
            for (int i = 0; i < numberOfItemsPerIteration; i++) {
                long value = random.nextInt(10000);
                if (writeValues) {
                    dataInterface.increaseCount(value);
                } else {
                    dataInterface.readCount(value);
                }
            }
            long endTime = System.nanoTime();
            synchronized (numberOfItemsProcessed) {
                numberOfItemsProcessed.setValue(numberOfItemsProcessed.longValue() + numberOfItemsPerIteration);
                timeSpend.setValue(timeSpend.longValue() + (endTime - startTime));
            }
        }

        countDownLatch.countDown();
    }

}
