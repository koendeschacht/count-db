package be.bow.db.rocksdb;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.combinator.Combinator;
import be.bow.util.Utils;
import org.rocksdb.RocksDB;

import java.io.File;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
public class RocksDBDataInterfaceFactory extends DataInterfaceFactory {

    private final String directory;
    private final boolean usePatch;

    public RocksDBDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, String directory, boolean usePatch) {
        super(cachesManager, memoryManager);
        this.directory = directory;
        this.usePatch = usePatch;
        File libFile = findLibFile();
        if (libFile == null) {
            throw new RuntimeException("Could not find librocksdbjni.so");
        }
        Utils.addLibraryPath(libFile.getParentFile().getAbsolutePath());
        RocksDB.loadLibrary();
    }

    private File findLibFile() {
        File libFile = new File("./lib/rocksdb/linux-x86_64/librocksdbjni.so");
        if (libFile.exists()) {
            return libFile;
        }
        libFile = new File("./count-db/lib/rocksdb/linux-x86_64/librocksdbjni.so");
        if (libFile.exists()) {
            return libFile;
        }
        return null;
    }

    @Override
    protected <T> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        return new RocksDBDataInterface<>(nameOfSubset, objectClass, combinator, directory, usePatch);
    }
}
