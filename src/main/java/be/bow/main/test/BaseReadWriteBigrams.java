package be.bow.main.test;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.DatabaseCachingType;
import be.bow.db.filedb.FileDataInterfaceFactory;
import be.bow.db.kyoto.KyotoDataInterfaceFactory;
import be.bow.db.leveldb.LevelDBDataInterfaceFactory;
import be.bow.db.rocksdb.RocksDBDataInterfaceFactory;
import be.bow.ui.UI;
import be.bow.util.NumUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.*;
import java.util.concurrent.CountDownLatch;

public abstract class BaseReadWriteBigrams extends BaseSpeedTest {

    private static final int MILLION_ITEMS_TO_PROCESS = 1024;

    private static final File largeTextFile = new File("/home/koen/bow/data/wikipedia/enwiki-latest-pages-articles.xml");
    //    private static final File largeTextFile = new File("/home/koen/bow/data/wikipedia/nlwiki-20140113-pages-articles.xml");
    private static final File tmpDbDir = new File("/tmp/testDatabaseSpeed");

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private OpenFilesManager openFilesManager;
    @Autowired
    private MemoryManager memoryManager;

    public void run() {
        try {
            prepareTmpDir(tmpDbDir);

            UI.write("Testing batch writing / reading");
            testBatchWritingAndReading(new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
            testBatchWritingAndReading(new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
            testBatchWritingAndReading(new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
            testBatchWritingAndReading(new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
            testBatchWritingAndReading(new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", true), DatabaseCachingType.DIRECT);
//            testBatchWritingAndReading(new LMDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/lmDB"), DatabaseCachingType.DIRECT);

            UI.write("Testing mixed writing / reading");
            testMixedWritingReading(new LevelDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/levelDB"), DatabaseCachingType.DIRECT);
            testMixedWritingReading(new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/fileDb"), DatabaseCachingType.CACHED_AND_BLOOM);
            testMixedWritingReading(new KyotoDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/kyotoDB"), DatabaseCachingType.DIRECT);
            testMixedWritingReading(new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", false), DatabaseCachingType.DIRECT);
            testMixedWritingReading(new RocksDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/rocksBD", true), DatabaseCachingType.DIRECT);
//            testMixedWritingReading(new LMDBDataInterfaceFactory(cachesManager, memoryManager, tmpDbDir.getAbsolutePath() + "/lmDB"), DatabaseCachingType.DIRECT);

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

    private void testBatchWritingAndReading(DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = 1024 * 1024; items <= MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testBatchWritingAndReading(factory, type, largeTextFile, 8, items);
        }
        factory.close();
    }

    private void testBatchWritingAndReading(DataInterfaceFactory factory, DatabaseCachingType type, File largeTextFile, int numberOfThreads, final long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(type, factory);
        dataInterface.dropAllData();
        final BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        double writesPerSecond = readTextWithThreads(numberOfThreads, numberOfItems, dataInterface, rdr, 0.0).getWritesPerSecond();
        dataInterface.optimizeForReading();
        double readsPerSecond = readTextWithThreads(numberOfThreads, numberOfItems, dataInterface, rdr, 1.0).getReadsPerSecond();
        dataInterface.close();
        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
    }

    private void testMixedWritingReading(DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        for (long items = 1024 * 1024; items <= MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testMixedWritingReading(factory, type, largeTextFile, 8, items);
        }
        factory.close();
    }

    private void testMixedWritingReading(DataInterfaceFactory factory, DatabaseCachingType type, File largeTextFile, int numberOfThreads, final long numberOfItems) throws FileNotFoundException, InterruptedException {
        final DataInterface dataInterface = createDataInterface(type, factory);
        dataInterface.dropAllData();
        final BufferedReader rdr = new BufferedReader(new FileReader(largeTextFile));
        ReadTextResults readTextResults = readTextWithThreads(numberOfThreads, numberOfItems, dataInterface, rdr, 0.5);
        double writesPerSecond = readTextResults.getWritesPerSecond();
        double readsPerSecond = readTextResults.getReadsPerSecond();
        dataInterface.close();
        UI.write(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
    }

    private ReadTextResults readTextWithThreads(int numberOfThreads, long numberOfItems, DataInterface dataInterface, BufferedReader rdr, double fractionToRead) throws InterruptedException {
        final MutableLong numberOfItemsWritten = new MutableLong(0);
        final MutableLong numberOfItemsRead = new MutableLong(0);
        final MutableLong timeSpendWriting = new MutableLong(0);
        final MutableLong timeSpendReading = new MutableLong(0);
        final CountDownLatch countDownLatchWrites = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            new ReadTextThread(this, numberOfItemsWritten, numberOfItemsRead, timeSpendWriting, timeSpendReading, numberOfItems, rdr, dataInterface, countDownLatchWrites, fractionToRead).start();
        }
        countDownLatchWrites.await();
        dataInterface.flush();
        double readsPerSecond = numberOfItemsRead.longValue() * 1e9 / timeSpendReading.longValue();
        double writesPerSecond = numberOfItemsWritten.longValue() * 1e9 / timeSpendWriting.longValue();
        return new ReadTextResults(readsPerSecond, writesPerSecond);
    }

    protected abstract DataInterface createDataInterface(DatabaseCachingType type, DataInterfaceFactory factory);

    protected abstract void doRead(String prev, String word, DataInterface dataInterface);

    protected abstract void doWrite(String prev, String word, DataInterface dataInterface);

    private static class ReadTextResults {
        private double readsPerSecond;
        private double writesPerSecond;

        private ReadTextResults(double readsPerSecond, double writesPerSecond) {
            this.readsPerSecond = readsPerSecond;
            this.writesPerSecond = writesPerSecond;
        }

        public double getReadsPerSecond() {
            return readsPerSecond;
        }

        public double getWritesPerSecond() {
            return writesPerSecond;
        }
    }

}
