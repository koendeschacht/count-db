package be.bagofwords.virtualfile.remote;

import be.bagofwords.application.MinimalApplicationDependencies;
import be.bagofwords.application.SocketServer;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.util.Utils;
import be.bagofwords.virtualfile.VirtualFile;
import be.bagofwords.virtualfile.local.LocalFileService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class TestRemoteFileService {

    @Test
    public void testWriteAndRead() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("data_directory", "/tmp/myFiles");
        config.put("socket_port", "1208");
        ApplicationContext context = new ApplicationContext(config);
        context.registerBean(MinimalApplicationDependencies.class);
        context.registerBean(SocketServer.class);
        context.registerBean(LocalFileService.class);
        context.registerBean(RemoteFileServer.class);
        Utils.threadSleep(1000); //Make sure server has started
        RemoteFileService remoteFileService = new RemoteFileService(context);
        VirtualFile dir = remoteFileService.getRootDirectory();
        VirtualFile file = dir.getFile("test1");
        OutputStream os1 = file.createOutputStream();
        IOUtils.write("test", os1);
        os1.close();
        BufferedReader rdr = new BufferedReader(new InputStreamReader(file.createInputStream()));
        Assert.assertEquals("test", rdr.readLine());
        rdr.close();
        context.terminate();
    }

}