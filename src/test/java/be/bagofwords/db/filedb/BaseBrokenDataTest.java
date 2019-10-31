package be.bagofwords.db.filedb;

import be.bagofwords.counts.Counter;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DatabaseCachingType;
import be.bagofwords.logging.Log;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class BaseBrokenDataTest {

    protected static final String DIRECTORY = "/tmp/testDIBrokenMetaData";

    private ApplicationContext applicationContext;
    protected FileDataInterfaceFactory factory;

    @Before
    public void setup() throws IOException {
        if (applicationContext != null) {
            applicationContext.close();
        }
        removeAllData();
        applicationContext = new ApplicationContext();
        applicationContext.setProperty("data_directory", DIRECTORY);
        factory = new FileDataInterfaceFactory(applicationContext);
    }

    @After
    public void tearDown() throws IOException {
        applicationContext.close();
        removeAllData();
    }

    protected int writeSomeData() {
        DataInterface<String> dataInterface = factory.dataInterface("test", String.class).caching(DatabaseCachingType.DIRECT).create();
        String longString = createLongString();
        int stringsWritten = 0;
        while (!foundMultipleFilesForBucket()) {
            for (int i = 0; i < 1000; i++) {
                dataInterface.write(createKey(stringsWritten), longString);
                stringsWritten++;
            }
            dataInterface.optimizeForReading();
        }
        dataInterface.close();
        Log.i("Wrote " + stringsWritten + " strings");
        return stringsWritten;
    }

    protected long createKey(long index) {
        return index << 9 + 1L;
    }

    private boolean foundMultipleFilesForBucket() {
        File dataInterfaceDirectory = new File(DIRECTORY, "test");
        Counter<Integer> bucketFileCounts = new Counter<>();
        for (File file : dataInterfaceDirectory.listFiles()) {
            FileNameInfo info = FileDataInterface.parseFileName(file.getName());
            if (info != null) {
                bucketFileCounts.inc(info.bucketInd);
            }
            if (bucketFileCounts.get(bucketFileCounts.sortedKeys().get(0)) > 1) {
                return true;
            }
        }
        return false;
    }

    private void removeAllData() throws IOException {
        File dataDirectory = new File(DIRECTORY);
        if (dataDirectory.exists()) {
            FileUtils.deleteDirectory(dataDirectory);
        }
    }

    protected String createLongString() {
        String longString = "";
        for (int i = 0; i < 1000; i++) {
            longString += "some data ";
        }
        return longString;
    }

    protected FileBucket getFileBucketWithData(FileDataInterface.MetaFile fileData) {
        return fileData.getFileBuckets()
                .stream()
                .filter(fileBucket -> fileBucket.getFiles().size() > 1)
                .findFirst()
                .get();
    }

    protected void writeMetaFile(FileDataInterface.MetaFile metaFileData, File metaFile) throws IOException {
        String serialized = SerializationUtils.serializeObject(metaFileData);
        FileUtils.write(metaFile, serialized, StandardCharsets.UTF_8);
    }

    protected FileDataInterface.MetaFile readMetaFile(File metaFile) throws IOException {
        String result = FileUtils.readFileToString(metaFile, StandardCharsets.UTF_8);
        return SerializationUtils.deserializeObject(result, FileDataInterface.MetaFile.class);
    }
}
