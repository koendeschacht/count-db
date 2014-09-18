package be.bow.main.test;

import be.bow.db.DataInterface;
import be.bow.text.WordIterator;
import be.bow.util.SafeThread;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
class ReadTextThread extends SafeThread {

    private BaseReadWriteBigrams baseReadWriteBigrams;
    private final MutableInt numberOfItemsWritten;
    private final long numberOfItems;
    private final long startTime;
    private final BufferedReader rdr;
    private final DataInterface dataInterface;

    public ReadTextThread(BaseReadWriteBigrams baseReadWriteBigrams, MutableInt numberOfItemsWritten, long numberOfItems, long startTime, BufferedReader rdr, DataInterface dataInterface) {
        super("ReadTextThread", false);
        this.baseReadWriteBigrams = baseReadWriteBigrams;
        this.numberOfItemsWritten = numberOfItemsWritten;
        this.numberOfItems = numberOfItems;
        this.startTime = startTime;
        this.rdr = rdr;
        this.dataInterface = dataInterface;
    }

    public void runInt() throws IOException {
        char[] textBuffer = new char[200000];
        while (numberOfItemsWritten.longValue() < numberOfItems && (System.currentTimeMillis() - startTime) < BaseReadWriteBigrams.MAX_TIME) {
            int charsRead;
            synchronized (rdr) {
                charsRead = rdr.read(textBuffer);
            }
            if (charsRead < 0) {
                throw new RuntimeException("Could not read the text");
            }
            long numberOfItemsWrittenForThisText = 0;
            char[] actualTextBuffer = charsRead < textBuffer.length ? Arrays.copyOf(textBuffer, charsRead) : textBuffer;
            WordIterator wordIterator = new WordIterator(actualTextBuffer, Collections.<String>emptySet());
            String prev = null;
            while (wordIterator.hasNext()) {
                String word = wordIterator.next().toString().toLowerCase();
                baseReadWriteBigrams.doWrite(prev, word, dataInterface);
                numberOfItemsWrittenForThisText++;
                prev = word;
            }
            synchronized (numberOfItemsWritten) {
                numberOfItemsWritten.setValue(numberOfItemsWritten.longValue() + numberOfItemsWrittenForThisText);
            }
        }
    }
}
