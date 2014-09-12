package be.bow.db.leveldb;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.combinator.Combinator;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;

import java.io.File;

public class LevelDBDataInterfaceFactory extends DataInterfaceFactory {

    private final String directory;

    public LevelDBDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(cachesManager, memoryManager);
        this.directory = directory;
        File dirFile = new File(directory);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
    }

    @Override
    protected <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        return new LevelDBDataInterface<>(directory, nameOfSubset, objectClass, combinator);
    }
}
