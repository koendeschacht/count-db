package be.bagofwords.main.tests.bigrams;

import be.bagofwords.db.DataInterface;
import be.bagofwords.text.WordIterator;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.SafeThread;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
class BigramTestsThread extends SafeThread {

    private final MutableLong numberOfItemsWritten;
    private final long numberOfItems;
    private final BufferedReader rdr;
    private final DataInterface dataInterface;
    private final DataType dataType;
    private CountDownLatch countDownLatch;
    private final boolean readData;

    public BigramTestsThread(DataType dataType, MutableLong numberOfItemsWritten, long numberOfItems, BufferedReader rdr, DataInterface dataInterface, CountDownLatch countDownLatch, boolean readData) {
        super("ReadTextThread", false);
        this.numberOfItemsWritten = numberOfItemsWritten;
        this.numberOfItems = numberOfItems;
        this.rdr = rdr;
        this.dataInterface = dataInterface;
        this.countDownLatch = countDownLatch;
        this.readData = readData;
        this.dataType = dataType;
    }

    public void runInt() throws IOException {
        char[] textBuffer = new char[200000];
        while (numberOfItemsWritten.longValue() < numberOfItems) {
            int charsRead;
            synchronized (rdr) {
                charsRead = rdr.read(textBuffer);
            }
            if (charsRead < 0) {
                throw new RuntimeException("Could not read the text");
            }
            long numOfWordsInText = 0;
            char[] actualTextBuffer = charsRead < textBuffer.length ? Arrays.copyOf(textBuffer, charsRead) : textBuffer;
            WordIterator wordIterator = new WordIterator(new String(actualTextBuffer), Collections.<String>emptySet());
            String prev = null;
            while (wordIterator.hasNext()) {
                String word = wordIterator.next().toString().toLowerCase();
                if (prev != null) {
                    if (readData) {
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
            synchronized (numberOfItemsWritten) {
                numberOfItemsWritten.setValue(numberOfItemsWritten.longValue() + numOfWordsInText);
            }
        }

        countDownLatch.countDown();
    }

}
