package be.bow.main.test;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseCachingType;
import be.bow.db.filedb.FileDataInterfaceFactory;
import be.bow.db.leveldb.LevelDBDataInterfaceFactory;
import be.bow.db.memory.InMemoryDataInterfaceFactory;
import be.bow.db.remote.RemoteDatabaseInterfaceFactory;
import be.bow.text.WordIterator;
import be.bow.ui.UI;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public abstract class BaseReadWriteBigrams extends BaseSpeedTest {

    private static final long CHARS_TO_READ = 100 * 1024 * 1024;
    private static final File largeTextFile = new File("/home/koen/bow/data/wikipedia/nlwiki-20140113-pages-articles.xml");
    private static final File tmpDbDir = new File("/tmp/testDatabaseSpeed");

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private OpenFilesManager openFilesManager;
    @Autowired
    private MemoryManager memoryManager;

    public void run() {
        try {
            warmupTextFile(largeTextFile);
            prepareTmpDir(tmpDbDir);
            LevelDBDataInterfaceFactory levelDBDataInterfaceFactory = new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB");
            FileDataInterfaceFactory fileDataInterfaceFactory = new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb");
            RemoteDatabaseInterfaceFactory remoteDatabaseInterfaceFactory = new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, "localhost", 1208);
            InMemoryDataInterfaceFactory inMemoryDataInterfaceFactory = new InMemoryDataInterfaceFactory(cachesManager, memoryManager);

            List<TestResult> testResults = new ArrayList<>();

            for (int numberOfThreads = 1; numberOfThreads <= 64; numberOfThreads *= 2) {
                doTest(testResults, levelDBDataInterfaceFactory, DatabaseCachingType.DIRECT, largeTextFile, numberOfThreads);
                doTest(testResults, fileDataInterfaceFactory, DatabaseCachingType.CACHED_AND_BLOOM, largeTextFile, numberOfThreads);
                doTest(testResults, remoteDatabaseInterfaceFactory, DatabaseCachingType.CACHED_AND_BLOOM, largeTextFile, numberOfThreads);
                doTest(testResults, inMemoryDataInterfaceFactory, DatabaseCachingType.DIRECT, largeTextFile, numberOfThreads);
            }

            printTestResults(testResults);
            levelDBDataInterfaceFactory.close();
            fileDataInterfaceFactory.close();
            remoteDatabaseInterfaceFactory.close();
        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }

    private static void prepareTmpDir(File tmpDbDir) throws IOException {
        if (!tmpDbDir.exists()) {
            boolean success = tmpDbDir.mkdir();
            if (!success) {
                throw new RuntimeException("Failed to create db dir " + tmpDbDir.getAbsolutePath());
            }
        } else {
            FileUtils.deleteDirectory(tmpDbDir);
        }
    }

    private static void warmupTextFile(File largeTextFile) throws IOException {
        UI.write("Warming up text file");
        BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        long charsRead = 0;
        while (charsRead < CHARS_TO_READ * 2) {
            charsRead += rdr.readLine().length();
        }
        rdr.close();
        UI.write("Finished warming up text file");
    }

    private static void printTestResults(List<TestResult> testResults) {
        for (TestResult testResult : testResults) {
            UI.write(testResult.getFactory().getClass().getSimpleName() + " " + testResult.getType() + " " + testResult.getAvgWrite() + " " + testResult.getAvgRead());
        }
    }

    public void doTest(List<TestResult> testResults, DataInterfaceFactory factory, DatabaseCachingType type, File largeTextFile, int numberOfThreads) throws InterruptedException, IOException {
        testResults.add(testWritingAndReading(factory, type, largeTextFile, numberOfThreads));
    }

    public TestResult testWritingAndReading(DataInterfaceFactory factory, DatabaseCachingType type, File largeTextFile, int numberOfThreads) throws InterruptedException, FileNotFoundException {
        UI.write("Starting tests for " + type + " " + factory.getClass().getSimpleName() + " with " + numberOfThreads + " threads.");
        final DataInterface dataInterface = createDataInterface(type, factory);
        dataInterface.dropAllData();
        final MutableInt numOfWrites = new MutableInt(0);
        final MutableInt charsReadForWrite = new MutableInt(0);
        final long startOfWrite = System.currentTimeMillis();
        final CountDownLatch countDownLatchWrites = new CountDownLatch(numberOfThreads);
        final BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        for (int i = 0; i < numberOfThreads; i++) {
            new Thread() {
                public void run() {
                    char[] textBuffer = new char[200000];
                    try {
                        while (charsReadForWrite.longValue() < CHARS_TO_READ) {
                            int charsRead;
                            synchronized (rdr) {
                                charsRead = rdr.read(textBuffer);
                                charsReadForWrite.setValue(charsReadForWrite.longValue() + charsRead);
                            }
                            char[] actualTextBuffer = charsRead < textBuffer.length ? Arrays.copyOf(textBuffer, charsRead) : textBuffer;
                            WordIterator wordIterator = new WordIterator(actualTextBuffer, Collections.<String>emptySet());
                            String prev = null;
                            while (wordIterator.hasNext()) {
                                String word = wordIterator.next().toString().toLowerCase();
                                doWrite(prev, word, dataInterface);
                                numOfWrites.increment();
                                prev = word;
                            }
                        }
                    } catch (Exception exp) {
                        UI.writeError("Received exception in writing thread ", exp);
                    }
                    countDownLatchWrites.countDown();

                }
            }.start();
        }
        countDownLatchWrites.await();
        dataInterface.flush();
        UI.write("Write phase finished, starting read phase");
        long taken = (System.currentTimeMillis() - startOfWrite);
        double avgWrite = taken / (double) numOfWrites.intValue();
        final long startOfRead = System.currentTimeMillis();
        final MutableInt numOfReads = new MutableInt(0);
        final MutableInt charsReadForReads = new MutableInt(0);
        final CountDownLatch countDownLatchReads = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            new Thread() {
                public void run() {
                    char[] textBuffer = new char[200000];
                    try {
                        while (charsReadForReads.longValue() < CHARS_TO_READ) {
                            int charsRead;
                            synchronized (rdr) {
                                charsRead = rdr.read(textBuffer);
                                charsReadForReads.setValue(charsReadForReads.longValue() + charsRead);
                            }
                            char[] actualTextBuffer = charsRead < textBuffer.length ? Arrays.copyOf(textBuffer, charsRead) : textBuffer;
                            WordIterator wordIterator = new WordIterator(actualTextBuffer, Collections.<String>emptySet());
                            String prev = null;
                            while (wordIterator.hasNext()) {
                                String word = wordIterator.next().toString().toLowerCase();
                                doRead(prev, word, dataInterface);
                                numOfReads.increment();
                                prev = word;
                            }
                        }
                    } catch (Exception exp) {
                        UI.writeError("Received exception in reading thread ", exp);
                    }
                    countDownLatchReads.countDown();
                }
            }.start();
        }
        countDownLatchReads.await();
        taken = (System.currentTimeMillis() - startOfRead);
        double avgRead = taken / (double) numOfReads.intValue();
        UI.write("Finished read phase, closing data interface");
        dataInterface.close();
        return new TestResult(type, factory, numberOfThreads, avgRead, avgWrite);
    }

    protected abstract DataInterface createDataInterface(DatabaseCachingType type, DataInterfaceFactory factory);

    protected abstract void doRead(String prev, String word, DataInterface dataInterface);

    protected abstract void doWrite(String prev, String word, DataInterface dataInterface);

    private static class TestResult {
        private final DatabaseCachingType type;
        private final DataInterfaceFactory factory;
        private final int numberOfThreads;
        private final double avgRead;
        private final double avgWrite;

        private TestResult(DatabaseCachingType type, DataInterfaceFactory factory, int numberOfThreads, double avgRead, double avgWrite) {
            this.type = type;
            this.factory = factory;
            this.avgRead = avgRead;
            this.avgWrite = avgWrite;
            this.numberOfThreads = numberOfThreads;
        }

        private DatabaseCachingType getType() {
            return type;
        }

        private DataInterfaceFactory getFactory() {
            return factory;
        }

        private double getAvgRead() {
            return avgRead;
        }

        private double getAvgWrite() {
            return avgWrite;
        }

        public int getNumberOfThreads() {
            return numberOfThreads;
        }
    }


}
