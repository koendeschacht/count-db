package be.bow.main.tests.random;

import be.bow.application.ApplicationManager;
import be.bow.application.MainClass;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseCachingType;
import be.bow.db.combinator.LongCombinator;
import be.bow.db.filedb.FileDataInterfaceFactory;
import be.bow.db.kyoto.KyotoDataInterfaceFactory;
import be.bow.db.leveldb.LevelDBDataInterfaceFactory;
import be.bow.db.rocksdb.RocksDBDataInterfaceFactory;
import be.bow.main.tests.TestsApplicationContextFactory;
import be.bow.ui.UI;
import be.bow.util.NumUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class RandomReadWritesMain implements MainClass {

    private static final int MILLION_ITEMS_TO_PROCESS = 8;
    private static final File tmpDbDir = new File("/tmp/testDatabaseSpeed");

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private OpenFilesManager openFilesManager;
    @Autowired
    private MemoryManager memoryManager;


    public static void main(String[] args) throws IOException, InterruptedException {
        ApplicationManager.runSafely(new TestsApplicationContextFactory(new RandomReadWritesMain()));
    }

    public void run() {
        try {
            prepareTmpDir(tmpDbDir);

            testWritingReading(new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
            testWritingReading(new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
            testWritingReading(new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
            testWritingReading(new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
            testWritingReading(new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", true), DatabaseCachingType.DIRECT);

        } catch (Exception exp) {
            throw new RuntimeException(exp);
        }
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

    private void testWritingReading(DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = 1024 * 1024; items <= MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testBatchWritingAndReading(factory, type, 8, items);
        }
        factory.close();
    }

    private void testBatchWritingAndReading(DataInterfaceFactory factory, DatabaseCachingType cachingType, int numberOfThreads, final long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(cachingType, factory);
        dataInterface.dropAllData();

        MutableLong numberOfItemsWritten = new MutableLong(0);
        MutableLong timeSpendWriting = new MutableLong(0);
        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            new RandomReadsWritesThread(numberOfItemsWritten, timeSpendWriting, numberOfItems, dataInterface, countDownLatch, true).start();
        }
        countDownLatch.await();
        double writesPerSecond = numberOfItemsWritten.longValue() * 1e9 / timeSpendWriting.longValue();

        countDownLatch = new CountDownLatch(numberOfThreads);
        MutableLong numberOfItemsRead = new MutableLong(0);
        MutableLong timeSpendReading = new MutableLong(0);
        for (int i = 0; i < numberOfThreads; i++) {
            new RandomReadsWritesThread(numberOfItemsRead, timeSpendReading, numberOfItems, dataInterface, countDownLatch, false).start();
        }
        countDownLatch.await();
        double readsPerSecond = numberOfItemsRead.longValue() * 1e9 / timeSpendReading.longValue();

        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));

        dataInterface.close();
    }

    protected DataInterface createDataInterface(DatabaseCachingType cachingType, DataInterfaceFactory factory) {
        String dataInterfaceName = "readWriteBigrams_" + cachingType + "_" + factory.getClass().getSimpleName();
        return factory.createDataInterface(cachingType, dataInterfaceName, Long.class, new LongCombinator());
    }

}
