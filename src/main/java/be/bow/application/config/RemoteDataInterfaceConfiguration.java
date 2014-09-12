package be.bow.application.config;

import be.bow.application.environment.OnionDBEnvironmentProperties;
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
    public DataInterfaceFactory dataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, OnionDBEnvironmentProperties environmentProperties) {
        return new RemoteDatabaseInterfaceFactory(cachesManager, memoryManager, environmentProperties.getDatabaseServerAddress(), 1208);
    }

    @Bean
    @Autowired
    public VirtualFileService virtualFileService(OnionDBEnvironmentProperties environmentProperties) {
        return new RemoteFileService(environmentProperties.getDatabaseServerAddress());
    }

}
