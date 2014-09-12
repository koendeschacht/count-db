package be.bow.application.config;

import be.bow.application.environment.CountDBEnvironmentProperties;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.filedb4.FileDataInterfaceFactory;
import be.bow.virtualfile.VirtualFileService;
import be.bow.virtualfile.local.LocalFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

public class FileDataInterfaceConfiguration {

    @Bean
    @Autowired
    public DataInterfaceFactory dataInterfaceFactory(OpenFilesManager openFilesManager, CachesManager cachesManager, MemoryManager memoryManager, CountDBEnvironmentProperties environmentProperties) {
        return new FileDataInterfaceFactory(openFilesManager, cachesManager, memoryManager, environmentProperties.getDataDirectory() + "server/");
    }

    @Bean
    @Autowired
    public VirtualFileService virtualFileService(CountDBEnvironmentProperties environmentProperties) {
        return new LocalFileService(environmentProperties.getDataDirectory() + "virtualFiles/");
    }

}
