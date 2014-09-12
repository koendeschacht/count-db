package be.bow.application.config;

import be.bow.application.environment.OnionDBEnvironmentProperties;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.filedb4.FileDataInterfaceFactory;
import be.bow.virtualfile.VirtualFileService;
import be.bow.virtualfile.local.LocalFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

public class LocalDataInterfaceConfiguration {

    @Bean
    @Autowired
    public DataInterfaceFactory dataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, OnionDBEnvironmentProperties environmentProperties) {
        return new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, environmentProperties.getDataDirectory() + "server/");
    }

    @Bean
    @Autowired
    public VirtualFileService virtualFileService(OnionDBEnvironmentProperties environmentProperties) {
        return new LocalFileService(environmentProperties.getDataDirectory() + "virtualFiles/");
    }

}
