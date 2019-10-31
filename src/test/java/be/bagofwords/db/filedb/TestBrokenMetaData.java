package be.bagofwords.db.filedb;

import be.bagofwords.db.DataInterface;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestBrokenMetaData extends BaseBrokenDataTest {

    @Test
    public void testRemovedMetaDataFile() {
        int stringsWritten = writeSomeData();
        File metaFile = new File(DIRECTORY, "test/META_FILE");
        assertTrue(metaFile.delete());
        DataInterface<String> dataInterface = factory.createDataInterface("test", String.class);
        String longString = createLongString();
        for (int i = 0; i < stringsWritten; i++) {
            assertEquals(longString, dataInterface.read(createKey(i)));
        }
    }

    @Test
    public void testBrokenMetaData() throws IOException {
        int stringsWritten = writeSomeData();
        File metaFile = new File(DIRECTORY, "test/META_FILE");
        FileDataInterface.MetaFile metaFileData = readMetaFile(metaFile);
        FileBucket bucket = getFileBucketWithData(metaFileData);
        while (!bucket.getFiles().isEmpty()) {
            bucket.getFiles().remove(0);
        }
        writeMetaFile(metaFileData, metaFile);
        DataInterface<String> dataInterface = factory.createDataInterface("test", String.class);
        String longString = createLongString();
        for (int i = 0; i < stringsWritten; i++) {
            assertEquals(longString, dataInterface.read(createKey(i)));
        }
    }

    @Test
    public void testClearlyIncorrectMetaData() throws IOException {
        int stringsWritten = writeSomeData();
        File metaFile = new File(DIRECTORY, "test/META_FILE");
        FileDataInterface.MetaFile metaFileData = readMetaFile(metaFile);
        FileBucket bucket = getFileBucketWithData(metaFileData);
        bucket.getFiles().set(0, new FileInfo(bucket.getIndex(), 0, 0, 0, 0));
        writeMetaFile(metaFileData, metaFile);
        DataInterface<String> dataInterface = factory.createDataInterface("test", String.class);
        String longString = createLongString();
        for (int i = 0; i < stringsWritten; i++) {
            assertEquals(longString, dataInterface.read(createKey(i)));
        }
    }

    @Test
    public void testMetaDataIncorrectSize() throws IOException {
        int stringsWritten = writeSomeData();
        File metaFile = new File(DIRECTORY, "test/META_FILE");
        FileDataInterface.MetaFile metaFileData = readMetaFile(metaFile);
        FileBucket bucket = getFileBucketWithData(metaFileData);
        FileInfo info = bucket.getFiles().get(0);
        bucket.getFiles().set(0, new FileInfo(bucket.getIndex(), info.getFirstKey(), info.getLastKey(), info.getReadSize() - 10, info.getWriteSize() - 10));
        writeMetaFile(metaFileData, metaFile);
        DataInterface<String> dataInterface = factory.createDataInterface("test", String.class);
        String longString = createLongString();
        for (int i = 0; i < stringsWritten; i++) {
            assertEquals(longString, dataInterface.read(createKey(i)));
        }
    }

}
