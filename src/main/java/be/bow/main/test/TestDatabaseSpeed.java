package be.bow.main.test;

import be.bow.application.MainClass;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.text.WordIterator;
import be.bow.ui.UI;
import be.bow.util.Utils;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseCachingType;
import be.bow.db.filedb4.FileDataInterface;
import be.bow.db.filedb4.FileDataInterfaceFactory;
import be.bow.db.leveldb.LevelDBDataInterfaceFactory;
import be.bow.db.memory.InMemoryDataInterfaceFactory;
import be.bow.db.remote.RemoteDatabaseInterfaceFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public abstract class TestDatabaseSpeed implements MainClass {

    private static final long CHARS_TO_READ = 200 * 1024 * 1024;
    private static final boolean includeInMemory = false;
    private static final int NUM_OF_THREADS = 20;
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
            FileDataInterfaceFactory fileDataInterfaceFactory = new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb4");
            RemoteDatabaseInterfaceFactory remoteDatabaseInterfaceFactory = new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, "localhost", 1208);

            List<TestResult> testResults = new ArrayList<>();
            doTest(testResults, levelDBDataInterfaceFactory, DatabaseCachingType.DIRECT, cachesManager, largeTextFile);
            doTest(testResults, fileDataInterfaceFactory, DatabaseCachingType.CACHED_AND_BLOOM, cachesManager, largeTextFile);
            doTest(testResults, remoteDatabaseInterfaceFactory, DatabaseCachingType.CACHED_AND_BLOOM, cachesManager, largeTextFile);

            for (DataInterface di : fileDataInterfaceFactory.getAllInterfaces()) {
                DataInterface implementing = di;
                while (implementing.getImplementingDataInterface() != null) {
                    implementing = implementing.getImplementingDataInterface();
                }
                FileDataInterface dataInterface4 = ((FileDataInterface) implementing);
                UI.write("Clean reads " + dataInterface4.numOfCleanReads + " dirty reads " + dataInterface4.numOfDirtyReads);
            }
            if (includeInMemory) {
                InMemoryDataInterfaceFactory inMemoryDataInterfaceFactory = new InMemoryDataInterfaceFactory(cachesManager, memoryManager);
                doTest(testResults, inMemoryDataInterfaceFactory, DatabaseCachingType.DIRECT, cachesManager, largeTextFile);
            }
            UI.write("Sorted by avg write time");
            Collections.sort(testResults, new Comparator<TestResult>() {
                @Override
                public int compare(TestResult o1, TestResult o2) {
                    return Double.compare(o1.getAvgWrite(), o2.getAvgWrite());
                }
            });
            printTestResults(testResults);
            UI.write("Sorted by avg read time");
            Collections.sort(testResults, new Comparator<TestResult>() {
                @Override
                public int compare(TestResult o1, TestResult o2) {
                    return Double.compare(o1.getAvgRead(), o2.getAvgRead());
                }
            });
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
            UI.write(testResult.getFactory().getClass().getSimpleName() + " " + testResult.getType() + " " + testResult.getNumOfWrites() + " " + testResult.getAvgWrite() + " " + testResult.getNumOfReads() + " " + testResult.getAvgRead());
        }
    }

    public void doTest(List<TestResult> testResults, DataInterfaceFactory factory, DatabaseCachingType type, CachesManager cachesManager, File largeTextFile) throws InterruptedException, IOException {
        testResults.add(testWritingAndReading(factory, type, largeTextFile));
        cachesManager.flushAll();
        System.gc();
        Utils.threadSleep(1000);
        System.gc();
    }

    public TestResult testWritingAndReading(DataInterfaceFactory factory, DatabaseCachingType type, File largeTextFile) throws InterruptedException, FileNotFoundException {
        UI.write("Starting tests for " + type + " " + factory.getClass().getSimpleName());
        final DataInterface dataInterface = createDataInterface(type, factory);
        dataInterface.dropAllData();
        final MutableInt numOfWrites = new MutableInt(0);
        final MutableInt charsReadForWrite = new MutableInt(0);
        final long startOfWrite = System.currentTimeMillis();
        final CountDownLatch countDownLatchWrites = new CountDownLatch(NUM_OF_THREADS);
        final BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        for (int i = 0; i < NUM_OF_THREADS; i++) {
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
        final CountDownLatch countDownLatchReads = new CountDownLatch(NUM_OF_THREADS);
        for (int i = 0; i < NUM_OF_THREADS; i++) {
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
        UI.write("Finished read phase, closing DI");
        dataInterface.close();
        return new TestResult(type, factory, avgRead, avgWrite, numOfReads.intValue(), numOfWrites.intValue());
    }

    protected abstract DataInterface createDataInterface(DatabaseCachingType type, DataInterfaceFactory factory);

    protected abstract void doRead(String prev, String word, DataInterface dataInterface);

    protected abstract void doWrite(String prev, String word, DataInterface dataInterface);

    private static class TestResult {
        private final DatabaseCachingType type;
        private final DataInterfaceFactory factory;
        private final long numOfReads;
        private final long numOfWrites;
        private final double avgRead;
        private final double avgWrite;

        private TestResult(DatabaseCachingType type, DataInterfaceFactory factory, double avgRead, double avgWrite, long numOfReads, long numOfWrites) {
            this.type = type;
            this.factory = factory;
            this.avgRead = avgRead;
            this.avgWrite = avgWrite;
            this.numOfReads = numOfReads;
            this.numOfWrites = numOfWrites;
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

        private long getNumOfReads() {
            return numOfReads;
        }

        private long getNumOfWrites() {
            return numOfWrites;
        }
    }


}
