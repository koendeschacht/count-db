package be.bagofwords.db.application.config;

import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.application.environment.FileCountDBEnvironmentProperties;
import be.bagofwords.application.file.OpenFilesManager;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.filedb.FileDataInterfaceFactory;
import be.bagofwords.virtualfile.VirtualFileService;
import be.bagofwords.virtualfile.local.LocalFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

public class FileDataInterfaceConfiguration {

    @Bean
    @Autowired
    public DataInterfaceFactory dataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, FileCountDBEnvironmentProperties environmentProperties) {
        return new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, environmentProperties.getDataDirectory() + "server/");
    }

    @Bean
    @Autowired
    public VirtualFileService virtualFileService(FileCountDBEnvironmentProperties environmentProperties) {
        return new LocalFileService(environmentProperties.getDataDirectory() + "virtualFiles/");
    }

}
