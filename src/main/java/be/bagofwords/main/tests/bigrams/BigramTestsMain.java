package be.bagofwords.main.tests.bigrams;

import be.bagofwords.application.ApplicationManager;
import be.bagofwords.application.MainClass;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.main.tests.TestsApplicationContextFactory;
import be.bagofwords.text.WordIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.NumUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

public class BigramTestsMain implements MainClass {

    private static final long MIN_MILLION_ITEMS_TO_PROCESS = 1;
    private static final long MAX_MILLION_ITEMS_TO_PROCESS = 128;

    private static final File tmpDbDir = new File("/tmp/testBigramCounts");

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private MemoryManager memoryManager;

    private final File largeTextFile;
    private final File bigramFile;

    public BigramTestsMain(File largeTextFile, File bigramFile) {
        this.largeTextFile = largeTextFile;
        this.bigramFile = bigramFile;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            UI.writeError("Please provide the path to a large text file");
        } else {
            ApplicationManager.runSafely(new TestsApplicationContextFactory(new BigramTestsMain(new File(args[0]), new File("/tmp/bigrams.bin"))));
        }
    }

    public void run() {
        try {
            prepareTmpDir(tmpDbDir);
            prepareBigrams();

            runAllTests(DataType.LONG_COUNT);
//            runAllTests(DataType.SERIALIZED_OBJECT);

        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }

    private void prepareBigrams() throws IOException {
        if (!bigramFile.exists() || bigramFile.length() == 0) {
            UI.write("Writing bigrams in " + largeTextFile.getAbsolutePath() + " to " + bigramFile.getAbsolutePath());
            BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(bigramFile)));
            long numOfBigramsWritten = 0;
            long bigramsToRead = MAX_MILLION_ITEMS_TO_PROCESS * 1000 * 1000 * 3;
            while (rdr.ready() && numOfBigramsWritten < bigramsToRead) {
                WordIterator wordIterator = new WordIterator(rdr.readLine(), Collections.<String>emptySet());
                String prev = null;
                while (wordIterator.hasNext()) {
                    String word = wordIterator.next().toString().toLowerCase();
                    if (prev != null) {
                        long bigram = HashUtils.hashCode(prev, " ", word);
                        dos.writeLong(bigram);
                        numOfBigramsWritten++;
                    }
                    prev = word;
                }
            }
            IOUtils.closeQuietly(rdr);
            IOUtils.closeQuietly(dos);
            UI.write("Finished writing bigrams.");
        }
    }

    private void runAllTests(DataType dataType) throws InterruptedException, FileNotFoundException {
        UI.write("Testing batch writing / reading for data type " + dataType);
//        testSeparateWritingReading(dataType, new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
        testSeparateWritingReading(dataType, new FileDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
//        testSeparateWritingReading(dataType, new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, "localhost", 1208), DatabaseCachingType.CACHED_AND_BLOOM);
//        testSeparateWritingReading(dataType, new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
//        testSeparateWritingReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
//        testSeparateWritingReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBDPatched", true), DatabaseCachingType.DIRECT);
        //testBatchWritingAndReading(dataType, new LMDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/lmDB"), DatabaseCachingType.DIRECT); --> too slow

        UI.write("Testing mixed writing / reading for data type " + dataType);
//        testMixedWritingReading(dataType, new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
//        testMixedWritingReading(dataType, new FileDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
//        testSeparateWritingReading(dataType, new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, "localhost", 1208), DatabaseCachingType.CACHED_AND_BLOOM);
//        testMixedWritingReading(dataType, new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
//        testMixedWritingReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
//        testMixedWritingReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBDPatched", true), DatabaseCachingType.DIRECT);
//        testMixedWritingReading(dataType, new LMDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/lmDB"), DatabaseCachingType.DIRECT); --> too slow
    }

    private static void prepareTmpDir(File tmpDbDir) throws IOException {
        if (tmpDbDir.exists()) {
            FileUtils.deleteDirectory(tmpDbDir);
        }
        boolean success = tmpDbDir.mkdirs();
        if (!success) {
            throw new RuntimeException("Failed to create db dir " + tmpDbDir.getAbsolutePath());
        }
    }

    private void testSeparateWritingReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = MIN_MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items <= MAX_MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testSeparateWritingReading(dataType, factory, type, 8, items);
        }
        factory.terminate();
    }

    private void testSeparateWritingReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType cachingType, int numberOfThreads, long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(dataType, cachingType, factory);
        dataInterface.dropAllData();
        final DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(bigramFile)));

        //write data
        MutableLong numberOfItemsWritten = new MutableLong(0);
        CountDownLatch writeLatch = new CountDownLatch(numberOfThreads);
        long startOfWrite = System.nanoTime();
        for (int i = 0; i < numberOfThreads; i++) {
            new BigramTestsThread(dataType, numberOfItemsWritten, numberOfItems, inputStream, dataInterface, writeLatch, false).start();
        }
        writeLatch.await();
        dataInterface.flush();
        long endOfWrite = System.nanoTime();
        double writesPerSecond = numberOfItemsWritten.longValue() * 1e9 / (endOfWrite - startOfWrite);

        //read data (we don't read more then 200M of items, since this is plenty to get an accurate estimate of reading speed)
        dataInterface.optimizeForReading();
        MutableLong numberOfItemsRead = new MutableLong(0);
        CountDownLatch readLatch = new CountDownLatch(numberOfThreads);
        long startOfRead = System.nanoTime();
        for (int i = 0; i < numberOfThreads; i++) {
            new BigramTestsThread(dataType, numberOfItemsRead, Math.min(200 * 1024 * 1024, numberOfItems), inputStream, dataInterface, readLatch, true).start();
        }
        readLatch.await();
        dataInterface.flush();
        long endOfRead = System.nanoTime();
        double readsPerSecond = numberOfItemsRead.longValue() * 1e9 / (endOfRead - startOfRead);

        dataInterface.close();
        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
    }

    private void testMixedWritingReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = MIN_MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items <= MAX_MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testMixedWritingReading(dataType, factory, type, largeTextFile, 8, items);
        }
        factory.terminate();
    }

    private void testMixedWritingReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType cachingType, File largeTextFile, int numberOfThreads, long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(dataType, cachingType, factory);
        dataInterface.dropAllData();
        final DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(bigramFile)));

        //first fill database by writing data
        MutableLong numberOfItemsWritten = new MutableLong(0);
        CountDownLatch writeLatch = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            new BigramTestsThread(dataType, numberOfItemsWritten, numberOfItems, inputStream, dataInterface, writeLatch, false).start();
        }
        writeLatch.await();
        dataInterface.flush();

        //now start threads that will write and read data simultaneously
        dataInterface.optimizeForReading();
        MutableLong numberOfItemsRead = new MutableLong(0);
        numberOfItemsWritten = new MutableLong(0);
        CountDownLatch readLatch = new CountDownLatch(numberOfThreads / 2);
        writeLatch = new CountDownLatch(numberOfThreads / 2);
        long start = System.nanoTime();
        for (int i = 0; i < numberOfThreads; i++) {
            boolean isReadThread = i % 2 == 0;
            new BigramTestsThread(dataType, isReadThread ? numberOfItemsRead : numberOfItemsWritten, Math.min(100 * 1024 * 1024, numberOfItems), inputStream, dataInterface, isReadThread ? readLatch : writeLatch, isReadThread).start();
        }
        readLatch.await(); //this assumes that reading data is faster than writing data.
        long endOfRead = System.nanoTime();
        writeLatch.await();
        dataInterface.flush();
        long endOfWrite = System.nanoTime();
        double readsPerSecond = numberOfItemsRead.longValue() * 1e9 / (endOfRead - start);
        double writesPerSecond = numberOfItemsWritten.longValue() * 1e9 / (endOfWrite - start);

        dataInterface.close();
        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
    }

    protected DataInterface createDataInterface(DataType dataType, DatabaseCachingType cachingType, DataInterfaceFactory factory) {
        String dataInterfaceName = "readWriteBigrams_" + dataType + "_" + cachingType + "_" + factory.getClass().getSimpleName();
        switch (dataType) {
            case LONG_COUNT:
                return factory.createDataInterface(cachingType, dataInterfaceName, Long.class, new LongCombinator());
            case SERIALIZED_OBJECT:
                return factory.createDataInterface(cachingType, dataInterfaceName, BigramCount.class, new BigramCountCombinator());
            default:
                throw new RuntimeException("Unknown data type " + dataType);
        }
    }

}
