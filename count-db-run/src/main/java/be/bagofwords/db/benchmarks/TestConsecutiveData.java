package be.bagofwords.db.benchmarks;

import be.bagofwords.application.MinimalApplicationDependencies;
import be.bagofwords.application.status.perf.ThreadSampleMonitor;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.db.combinator.LongCombinator;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.logging.Log;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.ApplicationManager;
import be.bagofwords.minidepi.annotations.Inject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static be.bagofwords.util.Utils.noException;

public class TestConsecutiveData implements Runnable {

    private static final File tmpDbDir = new File("/tmp/testConsecutiveData");

    public static void main(String[] args) throws IOException, InterruptedException {
        Map<String, String> config = new HashMap<>();
        prepareTmpDir(tmpDbDir);
        config.put("data_directory", tmpDbDir.getAbsolutePath());
        config.put("save.thread.samples.to.file", "true");
        config.put("location.for.saved.thread.samples", "/tmp/samples.txt");
        ApplicationManager.run(new TestConsecutiveData(), config);
    }

    @Inject
    private ApplicationContext context;
    @Inject
    private MinimalApplicationDependencies minimalApplicationDependencies;
    @Inject
    private ThreadSampleMonitor threadSampleMonitor;
    @Inject
    private FileDataInterfaceFactory fileDataInterfaceFactory;

    public void run() {
        noException(() -> {
            int numOfItems = 10000000;
            List<Long> consecutiveValues = createListOfConsecutiveValues(numOfItems);
            BaseDataInterface<Long> dataInterface = createDataInterface(DatabaseCachingType.CACHED, fileDataInterfaceFactory);
            Log.i("Consecutive items took " + timeWritingAndReadingValues(dataInterface, consecutiveValues));
            consecutiveValues.clear();
            List<Long> randomValues = createListOfRandomValues(numOfItems);
            Log.i("Random items took " + timeWritingAndReadingValues(dataInterface, randomValues));
        });
    }

    private List<Long> createListOfConsecutiveValues(int numOfItems) {
        List<Long> values = new ArrayList<>();
        for (int i = 0; i < numOfItems; i++) {
            values.add((long) i);
        }
        return values;
    }

    private long timeWritingAndReadingValues(BaseDataInterface<Long> dataInterface, List<Long> values) {
        dataInterface.dropAllData();
        long start = System.currentTimeMillis();
        for (int i = 0; i < values.size(); i++) {
            dataInterface.write(values.get(i), values.get(i));
        }
        for (int i = 0; i < values.size(); i++) {
            dataInterface.read(values.get(i));
        }
        return System.currentTimeMillis() - start;
    }

    private List<Long> createListOfRandomValues(int numOfItems) {
        Random random = new Random();
        List<Long> randomValues = new ArrayList<>();
        for (int i = 0; i < numOfItems; i++) {
            randomValues.add(random.nextLong());
        }
        return randomValues;
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

    protected BaseDataInterface<Long> createDataInterface(DatabaseCachingType cachingType, DataInterfaceFactory factory) {
        String dataInterfaceName = "readWriteRandom_" + cachingType + "_" + factory.getClass().getSimpleName();
        return factory.dataInterface(dataInterfaceName, Long.class).combinator(new LongCombinator()).caching(cachingType).create();
    }

}
