package be.bow.db.kyoto;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.combinator.Combinator;
import be.bow.util.Utils;

import java.io.File;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
public class KyotoDataInterfaceFactory extends DataInterfaceFactory {

    private final String directory;

    public KyotoDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(cachesManager, memoryManager);
        this.directory = directory;
        File libFile = findLibFile();
        if (libFile == null) {
            throw new RuntimeException("Could not find libkyotocabinet.so.16");
        }
        System.load(libFile.getAbsolutePath());
        Utils.addLibraryPath(libFile.getParentFile().getAbsolutePath());
    }

    private File findLibFile() {
        File libFile = new File("./lib/kyotocabinet-1.24/linux-x86_64/libkyotocabinet.so.16");
        if (libFile.exists()) {
            return libFile;
        }
        libFile = new File("./count-db/lib/kyotocabinet-1.24/linux-x86_64/libkyotocabinet.so.16");
        if (libFile.exists()) {
            return libFile;
        }
        return null;
    }


    @Override
    protected <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        return new KyotoDataInterface<>(nameOfSubset, directory, objectClass, combinator);
    }
}
