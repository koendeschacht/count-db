package be.bagofwords.db.interfaces.rocksdb;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.util.Utils;
import org.rocksdb.RocksDB;

import java.io.File;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/17/14.
 */
public class RocksDBDataInterfaceFactory extends BaseDataInterfaceFactory {

    private final String directory;
    private final boolean usePatch;

    public RocksDBDataInterfaceFactory(ApplicationContext applicationContext, boolean usePatch) {
        super(applicationContext);
        this.directory = applicationContext.getProperty("data_directory");
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
    protected <T> CoreDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporaryDataInterface) {
        return new RocksDBDataInterface<>(name, objectClass, combinator, objectSerializer, directory, usePatch, isTemporaryDataInterface);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return RocksDBDataInterface.class;
    }
}
