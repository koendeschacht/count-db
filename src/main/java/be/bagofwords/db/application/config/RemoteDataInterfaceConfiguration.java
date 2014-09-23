package be.bagofwords.db.application.config;

import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.remote.RemoteDatabaseInterfaceFactory;
import be.bagofwords.db.application.environment.RemoteCountDBEnvironmentProperties;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.virtualfile.VirtualFileService;
import be.bagofwords.virtualfile.remote.RemoteFileService;
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
