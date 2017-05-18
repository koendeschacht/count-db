package be.bagofwords.main.tests.uniform;

import be.bagofwords.db.DataInterface;
import be.bagofwords.util.SafeThread;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
class UniformDataTestsThread extends SafeThread {

    private final MutableLong numberOfItemsProcessed;
    private final long numberOfItems;
    private final DataInterface dataInterface;
    private CountDownLatch countDownLatch;
    private final boolean writeValues;

    public UniformDataTestsThread(MutableLong numberOfItemsProcessed, long numberOfItems, DataInterface dataInterface, CountDownLatch countDownLatch, boolean writeValues) {
        super("ReadTextThread", false);
        this.numberOfItemsProcessed = numberOfItemsProcessed;
        this.numberOfItems = numberOfItems;
        this.dataInterface = dataInterface;
        this.countDownLatch = countDownLatch;
        this.writeValues = writeValues;
    }

    public void runImpl() throws IOException {
        Random random = new Random();
        while (numberOfItemsProcessed.longValue() < numberOfItems) {
            long numberOfItemsPerIteration = numberOfItems / 10000;
            for (int i = 0; i < numberOfItemsPerIteration; i++) {
                long value = random.nextInt(1000000);
                if (writeValues) {
                    dataInterface.increaseCount(value);
                } else {
                    dataInterface.readCount(value);
                }
            }
            synchronized (numberOfItemsProcessed) {
                numberOfItemsProcessed.setValue(numberOfItemsProcessed.longValue() + numberOfItemsPerIteration);
            }
        }

        countDownLatch.countDown();
    }

}
