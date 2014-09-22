package be.bow.main.bigrams;

import be.bow.db.DataInterface;
import be.bow.text.WordIterator;
import be.bow.util.HashUtils;
import be.bow.util.SafeThread;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
class ReadTextThread extends SafeThread {

    private final MutableLong numberOfItemsWritten;
    private final MutableLong numberOfItemsRead;
    private final MutableLong timeSpendWriting;
    private final MutableLong timeSpendReading;
    private final long numberOfItems;
    private final BufferedReader rdr;
    private final DataInterface dataInterface;
    private final DataType dataType;
    private CountDownLatch countDownLatch;
    private final double fractionToRead;

    public ReadTextThread(DataType dataType, MutableLong numberOfItemsWritten, MutableLong numberOfItemsRead, MutableLong timeSpendWriting, MutableLong timeSpendReading, long numberOfItems, BufferedReader rdr, DataInterface dataInterface, CountDownLatch countDownLatch, double fractionToRead) {
        super("ReadTextThread", false);
        this.numberOfItemsWritten = numberOfItemsWritten;
        this.numberOfItemsRead = numberOfItemsRead;
        this.timeSpendReading = timeSpendReading;
        this.timeSpendWriting = timeSpendWriting;
        this.numberOfItems = numberOfItems;
        this.rdr = rdr;
        this.dataInterface = dataInterface;
        this.countDownLatch = countDownLatch;
        this.fractionToRead = fractionToRead;
        this.dataType = dataType;
    }

    public void runInt() throws IOException {
        Random random = new Random();
        char[] textBuffer = new char[200000];
        while (numberOfItemsWritten.longValue() + numberOfItemsRead.longValue() < numberOfItems) {
            int charsRead;
            synchronized (rdr) {
                charsRead = rdr.read(textBuffer);
            }
            if (charsRead < 0) {
                throw new RuntimeException("Could not read the text");
            }
            boolean readValuesInThisText = random.nextDouble() < fractionToRead;
            long startTime = System.nanoTime();
            long numOfWordsInText = 0;
            char[] actualTextBuffer = charsRead < textBuffer.length ? Arrays.copyOf(textBuffer, charsRead) : textBuffer;
            WordIterator wordIterator = new WordIterator(actualTextBuffer, Collections.<String>emptySet());
            String prev = null;
            while (wordIterator.hasNext()) {
                String word = wordIterator.next().toString().toLowerCase();
                if (prev != null) {
                    if (readValuesInThisText) {
                        if (dataType == DataType.LONG_COUNT) {
                            dataInterface.readCount(prev + " " + word);
                        } else {
                            dataInterface.read(HashUtils.hashCode(prev + " " + word));
                        }
                    } else {
                        if (dataType == DataType.LONG_COUNT) {
                            dataInterface.increaseCount(prev + " " + word);
                        } else {
                            dataInterface.write(HashUtils.hashCode(prev + " " + word), new BigramCount(prev, word));
                        }
                    }
                    numOfWordsInText++;
                }
                prev = word;
            }
            long endTime = System.nanoTime();
            MutableLong itemsWritten = readValuesInThisText ? numberOfItemsRead : numberOfItemsWritten;
            MutableLong timeSpend = readValuesInThisText ? timeSpendReading : timeSpendWriting;
            synchronized (itemsWritten) {
                itemsWritten.setValue(itemsWritten.longValue() + numOfWordsInText);
                timeSpend.setValue(timeSpend.longValue() + (endTime - startTime));
            }
        }

        countDownLatch.countDown();
    }

}
