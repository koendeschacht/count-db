package be.bagofwords.virtualfile.remote;

import be.bagofwords.virtualfile.VirtualFile;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

public class TestRemoteFileService {

    @Test
    public void testWriteAndRead() throws IOException {
        RemoteFileService remoteFileService = new RemoteFileService("localhost", 1209);
        VirtualFile dir = remoteFileService.getRootDirectory();
        VirtualFile file = dir.getFile("test1");
        OutputStream os1 = file.createOutputStream();
        IOUtils.write("test", os1);
        os1.close();
        BufferedReader rdr = new BufferedReader(new InputStreamReader(file.createInputStream()));
        Assert.assertEquals("test", rdr.readLine());
        rdr.close();
    }

}