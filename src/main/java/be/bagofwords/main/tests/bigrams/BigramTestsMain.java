package be.bagofwords.main.tests.bigrams;

import be.bagofwords.application.ApplicationManager;
import be.bagofwords.application.MainClass;
import be.bagofwords.application.file.OpenFilesManager;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.experimental.kyoto.KyotoDataInterfaceFactory;
import be.bagofwords.db.experimental.rocksdb.RocksDBDataInterfaceFactory;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.leveldb.LevelDBDataInterfaceFactory;
import be.bagofwords.main.tests.TestsApplicationContextFactory;
import be.bagofwords.ui.UI;
import be.bagofwords.util.NumUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public class BigramTestsMain implements MainClass {

    private static final int MILLION_ITEMS_TO_PROCESS = 8;
    private static final File tmpDbDir = new File("/tmp/testDatabaseSpeed");

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private OpenFilesManager openFilesManager;
    @Autowired
    private MemoryManager memoryManager;

    private final File largeTextFile;

    public BigramTestsMain(File largeTextFile) {
        this.largeTextFile = largeTextFile;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 1) {
            UI.writeError("Please provide the path to a large text file");
        } else {
            ApplicationManager.runSafely(new TestsApplicationContextFactory(new BigramTestsMain(new File(args[0]))));
        }
    }

    public void run() {
        try {
            UI.write("Reading " + largeTextFile.getAbsolutePath());
            prepareTmpDir(tmpDbDir);

            runAllTests(DataType.LONG_COUNT);
            runAllTests(DataType.SERIALIZED_OBJECT);

        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
    }

    private void runAllTests(DataType dataType) throws InterruptedException, FileNotFoundException {
        UI.write("Testing batch writing / reading for data type " + dataType);
        testBatchWritingAndReading(dataType, new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
        testBatchWritingAndReading(dataType, new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
        testBatchWritingAndReading(dataType, new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
        testBatchWritingAndReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
        testBatchWritingAndReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", true), DatabaseCachingType.DIRECT);
        //testBatchWritingAndReading(dataType, new LMDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/lmDB"), DatabaseCachingType.DIRECT); --> too slow

        UI.write("Testing mixed writing / reading for data type " + dataType);
        testMixedWritingReading(dataType, new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
        testMixedWritingReading(dataType, new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
        testMixedWritingReading(dataType, new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
        testMixedWritingReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
        testMixedWritingReading(dataType, new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", true), DatabaseCachingType.DIRECT);
        //testMixedWritingReading(dataType, new LMDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/lmDB"), DatabaseCachingType.DIRECT); --> too slow
    }

    private static void prepareTmpDir(File tmpDbDir) throws IOException {
        if (!tmpDbDir.exists()) {
            boolean success = tmpDbDir.mkdirs();
            if (!success) {
                throw new RuntimeException("Failed to create db dir " + tmpDbDir.getAbsolutePath());
            }
        } else {
            FileUtils.deleteDirectory(tmpDbDir);
        }
    }

    private void testBatchWritingAndReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = 1024 * 1024; items <= MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testBatchWritingAndReading(dataType, factory, type, largeTextFile, 8, items);
        }
        factory.close();
    }

    private void testBatchWritingAndReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType cachingType, File largeTextFile, int numberOfThreads, final long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(dataType, cachingType, factory);
        dataInterface.dropAllData();
        final BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        double writesPerSecond = readTextWithThreads(dataType, numberOfThreads, numberOfItems, dataInterface, rdr, 0.0).getWritesPerSecond();
        dataInterface.optimizeForReading();
        double readsPerSecond = readTextWithThreads(dataType, numberOfThreads, numberOfItems, dataInterface, rdr, 1.0).getReadsPerSecond();
        dataInterface.close();
        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
    }

    private void testMixedWritingReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = 1024 * 1024; items <= MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testMixedWritingReading(dataType, factory, type, largeTextFile, 8, items);
        }
        factory.close();
    }

    private void testMixedWritingReading(DataType dataType, DataInterfaceFactory factory, DatabaseCachingType cachingType, File largeTextFile, int numberOfThreads, final long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(dataType, cachingType, factory);
        dataInterface.dropAllData();
        final BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        ReadTextResults readTextResults = readTextWithThreads(dataType, numberOfThreads, numberOfItems, dataInterface, rdr, 0.5);
        double writesPerSecond = readTextResults.getWritesPerSecond();
        double readsPerSecond = readTextResults.getReadsPerSecond();
        dataInterface.close();
        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
    }

    private ReadTextResults readTextWithThreads(DataType dataType, int numberOfThreads, long numberOfItems, DataInterface dataInterface, BufferedReader rdr, double fractionToRead) throws InterruptedException {
        final MutableLong numberOfItemsWritten = new MutableLong(0);
        final MutableLong numberOfItemsRead = new MutableLong(0);
        final MutableLong timeSpendWriting = new MutableLong(0);
        final MutableLong timeSpendReading = new MutableLong(0);
        final CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            new ReadTextThread(dataType, numberOfItemsWritten, numberOfItemsRead, timeSpendWriting, timeSpendReading, numberOfItems, rdr, dataInterface, countDownLatch, fractionToRead).start();
        }
        countDownLatch.await();
        dataInterface.flush();
        double readsPerSecond = numberOfItemsRead.longValue() * 1e9 / timeSpendReading.longValue();
        double writesPerSecond = numberOfItemsWritten.longValue() * 1e9 / timeSpendWriting.longValue();
        return new ReadTextResults(readsPerSecond, writesPerSecond);
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
