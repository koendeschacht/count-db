package be.bagofwords.db.benchmarks.uniform;

import be.bagofwords.application.MinimalApplicationDependencies;
import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.experimental.kyoto.KyotoDataInterfaceFactory;
import be.bagofwords.db.experimental.rocksdb.RocksDBDataInterfaceFactory;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.leveldb.LevelDBDataInterfaceFactory;
import be.bagofwords.logging.Log;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.ApplicationManager;
import be.bagofwords.minidepi.annotations.Inject;
import be.bagofwords.util.NumUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static be.bagofwords.util.Utils.noException;

public class UniformDataTestsMain implements Runnable {

    private static final int MIN_MILLION_ITEMS_TO_PROCESS = 1;
    private static final int MAX_MILLION_ITEMS_TO_PROCESS = 128;

    private static final File tmpDbDir = new File("/tmp/testUniformData");

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        config.put("data_directory", tmpDbDir.getAbsolutePath());
        config.put("save.thread.samples.to.file", "true");
        config.put("location.for.saved.thread.samples", "/tmp/samples.txt");
        ApplicationManager.run(new UniformDataTestsMain(), config);
    }

    @Inject
    private ApplicationContext context;
    @Inject
    private MinimalApplicationDependencies minimalApplicationDependencies;
    @Inject
    private ThreadSampleMonitor threadSampleMonitor;


    public void run() {
        noException(() -> {
            prepareTmpDir(tmpDbDir);
            testWritingReading(new LevelDBDataInterfaceFactory(context), DatabaseCachingType.DIRECT);
            testWritingReading(new FileDataInterfaceFactory(context), DatabaseCachingType.CACHED_AND_BLOOM);
            testWritingReading(new KyotoDataInterfaceFactory(context), DatabaseCachingType.DIRECT);
            testWritingReading(new RocksDBDataInterfaceFactory(context, false), DatabaseCachingType.DIRECT);
            testWritingReading(new RocksDBDataInterfaceFactory(context, true), DatabaseCachingType.DIRECT);
        });
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

    private void testWritingReading(DataInterfaceFactory factory, DatabaseCachingType type) throws InterruptedException, FileNotFoundException {
        context.registerBean(factory);
        for (long items = MIN_MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items <= MAX_MILLION_ITEMS_TO_PROCESS * 1024 * 1024; items *= 2) {
            testBatchWritingAndReading(factory, type, 8, items);
        }
        factory.terminate();
    }

    private void testBatchWritingAndReading(DataInterfaceFactory factory, DatabaseCachingType cachingType, int numberOfThreads, final long numberOfItems) throws FileNotFoundException, InterruptedException {
        final BaseDataInterface dataInterface = createDataInterface(cachingType, factory);
        dataInterface.dropAllData();

        MutableLong numberOfItemsWritten = new MutableLong(0);
        long startOfWrite = System.nanoTime();
        CountDownLatch countDownLatch = new CountDownLatch(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            new UniformDataTestsThread(numberOfItemsWritten, numberOfItems, dataInterface, countDownLatch, true).start();
        }
        countDownLatch.await();
        dataInterface.flush();
        long endOfWrite = System.nanoTime();
        double writesPerSecond = numberOfItemsWritten.longValue() * 1e9 / (endOfWrite - startOfWrite);

        countDownLatch = new CountDownLatch(numberOfThreads);
        dataInterface.optimizeForReading();
        MutableLong numberOfItemsRead = new MutableLong(0);
        long startOfRead = System.nanoTime();
        for (int i = 0; i < numberOfThreads; i++) {
            new UniformDataTestsThread(numberOfItemsRead, numberOfItems, dataInterface, countDownLatch, false).start();
        }
        countDownLatch.await();
        long endOfRead = System.nanoTime();
        double readsPerSecond = numberOfItemsRead.longValue() * 1e9 / (endOfRead - startOfRead);

        Log.i(factory.getClass().getSimpleName() + " threads " + numberOfThreads + " items " + numberOfItems + " write " + NumUtils.fmt(writesPerSecond) + " read " + NumUtils.fmt(readsPerSecond));
        dataInterface.close();
    }

    protected BaseDataInterface createDataInterface(DatabaseCachingType cachingType, DataInterfaceFactory factory) {
        String dataInterfaceName = "readWriteRandom_" + cachingType + "_" + factory.getClass().getSimpleName();
        return factory.dataInterface(dataInterfaceName, Long.class).combinator(new LongCombinator()).caching(cachingType).create();
    }

}
