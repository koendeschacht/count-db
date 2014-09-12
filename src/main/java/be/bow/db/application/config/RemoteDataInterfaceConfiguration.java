package be.bow.db.application.config;

import be.bow.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.remote.RemoteDatabaseInterfaceFactory;
import be.bow.virtualfile.VirtualFileService;
import be.bow.virtualfile.remote.RemoteFileService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

public class RemoteDataInterfaceConfiguration {

    @Bean
    @Autowired
    public DataInterfaceFactory dataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, RemoteCountDBEnvironmentProperties environmentProperties) {
        return new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDatabaseServerAddress(), environmentProperties.getDataInterfaceServerPort());
    }

    @Bean
    @Autowired
    public VirtualFileService virtualFileService(RemoteCountDBEnvironmentProperties environmentProperties) {
        return new RemoteFileService(environmentProperties.getDatabaseServerAddress(), environmentProperties.getDataInterfaceServerPort());
    }

}
