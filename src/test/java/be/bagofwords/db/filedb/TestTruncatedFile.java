package be.bagofwords.db.filedb;

import be.bagofwords.db.DataInterface;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class TestTruncatedFile extends BaseBrokenDataTest {

    @Test
    public void testTruncatedFile() throws IOException {
        int stringsWritten = writeSomeData();
        for (File file : new File(DIRECTORY, "test").listFiles()) {
            FileNameInfo fileNameInfo = FileDataInterface.parseFileName(file.getName());
            if (file.length() > 0 && fileNameInfo != null) {
                //Is a regular data file with some data
                byte[] contents = FileUtils.readFileToByteArray(file);
                byte[] truncatedContents = Arrays.copyOf(contents, contents.length / 2);
                FileUtils.writeByteArrayToFile(file, truncatedContents);
                break;
            }
        }
        DataInterface<String> dataInterface = factory.createDataInterface("test", String.class);
        long stringsRead = dataInterface.stream().count();
        Assert.assertTrue(stringsRead < stringsWritten && stringsRead >= stringsWritten / 2);
    }

}
