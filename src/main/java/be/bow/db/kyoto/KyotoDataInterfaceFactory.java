package be.bow.db.kyoto;

import be.bow.application.memory.MemoryManager;
import be.bow.cache.CachesManager;
import be.bow.db.DataInterface;
import be.bow.db.DataInterfaceFactory;
import be.bow.db.combinator.Combinator;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Arrays;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
public class KyotoDataInterfaceFactory extends DataInterfaceFactory {

    private final String directory;

    public KyotoDataInterfaceFactory(CachesManager cachesManager, MemoryManager memoryManager, String directory) {
        super(cachesManager, memoryManager);
        File libFile = findLibFile();
        if (libFile == null) {
            throw new RuntimeException("Could not find libkyotocabinet.so.16");
        }
        System.load(libFile.getAbsolutePath());
        this.directory = directory;
        try {
            addLibraryPath(libFile.getParentFile().getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

    public static void addLibraryPath(String pathToAdd) throws Exception {
        try {
            final Field usrPathsField = ClassLoader.class.getDeclaredField("usr_paths");
            usrPathsField.setAccessible(true);

            //get array of paths
            final String[] paths = (String[]) usrPathsField.get(null);

            //check if the path to add is already present
            for (String path : paths) {
                if (path.equals(pathToAdd)) {
                    return;
                }
            }

            //add the new path
            final String[] newPaths = Arrays.copyOf(paths, paths.length + 1);
            newPaths[newPaths.length - 1] = pathToAdd;
            usrPathsField.set(null, newPaths);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator) {
        return new KyotoDataInterface<>(nameOfSubset, directory, objectClass, combinator);
    }
}
