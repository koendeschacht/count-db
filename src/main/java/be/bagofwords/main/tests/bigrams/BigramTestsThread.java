package be.bagofwords.main.tests.bigrams;

import be.bagofwords.db.DataInterface;
import be.bagofwords.util.SafeThread;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
class BigramTestsThread extends SafeThread {

    private final MutableLong numberOfItemsWritten;
    private final long numberOfItems;
    private final DataInputStream inputStream;
    private final DataInterface dataInterface;
    private final DataType dataType;
    private CountDownLatch countDownLatch;
    private final boolean readData;

    public BigramTestsThread(DataType dataType, MutableLong numberOfItemsWritten, long numberOfItems, DataInputStream dis, DataInterface dataInterface, CountDownLatch countDownLatch, boolean readData) {
        super("ReadTextThread", false);
        this.numberOfItemsWritten = numberOfItemsWritten;
        this.numberOfItems = numberOfItems;
        this.inputStream = dis;
        this.dataInterface = dataInterface;
        this.countDownLatch = countDownLatch;
        this.readData = readData;
        this.dataType = dataType;
    }

    public void runInt() throws IOException {
        while (numberOfItemsWritten.longValue() < numberOfItems) {
            List<Long> bigrams = new ArrayList<>(10000);
            synchronized (inputStream) {
                for (int i = 0; i < 10000; i++) {
                    bigrams.add(inputStream.readLong());
                }
            }
            for (Long bigram : bigrams) {
                if (readData) {
                    if (dataType == DataType.LONG_COUNT) {
                        dataInterface.readCount(bigram);
                    } else {
                        dataInterface.read(bigram);
                    }
                } else {
                    if (dataType == DataType.LONG_COUNT) {
                        dataInterface.increaseCount(bigram);
                    } else {
                        dataInterface.write(bigram, new BigramCount(bigram));
                    }
                }
            }
            synchronized (numberOfItemsWritten) {
                numberOfItemsWritten.setValue(numberOfItemsWritten.longValue() + bigrams.size());
            }
        }

        countDownLatch.countDown();
    }

}
