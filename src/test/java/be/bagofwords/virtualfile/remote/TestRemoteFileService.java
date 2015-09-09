package be.bagofwords.virtualfile.remote;

import be.bagofwords.application.BowTaskScheduler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.db.helper.UnitTestContextLoader;
import be.bagofwords.util.Utils;
import be.bagofwords.virtualfile.VirtualFile;
import be.bagofwords.virtualfile.VirtualFileService;
import be.bagofwords.virtualfile.local.LocalFileService;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = UnitTestContextLoader.class)
public class TestRemoteFileService {

    @Autowired
    private CachesManager cachesManager;
    @Autowired
    private MemoryManager memoryManager;
    @Autowired
    private BowTaskScheduler taskScheduler;
    @Autowired
    private RemoteCountDBEnvironmentProperties properties;

    @Test
    public void testWriteAndRead() throws IOException {
        VirtualFileService localFileService = new LocalFileService("/tmp/myFiles");
        RemoteFileServer remoteFileServer = new RemoteFileServer(localFileService, properties);
        remoteFileServer.start();
        Utils.threadSleep(1000); //Make sure server has started
        RemoteFileService remoteFileService = new RemoteFileService("localhost", properties.getVirtualFileServerPort());
        VirtualFile dir = remoteFileService.getRootDirectory();
        VirtualFile file = dir.getFile("test1");
        OutputStream os1 = file.createOutputStream();
        IOUtils.write("test", os1);
        os1.close();
        BufferedReader rdr = new BufferedReader(new InputStreamReader(file.createInputStream()));
        Assert.assertEquals("test", rdr.readLine());
        rdr.close();
        remoteFileServer.terminate();
    }

}