package be.bagofwords.db.experimental.lmdb;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.minidepi.ApplicationContext;
import org.fusesource.lmdbjni.Env;

import java.io.File;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/16/14.
 */
public class LMDBDataInterfaceFactory extends BaseDataInterfaceFactory {

    private Env env;
    private String directory;

    public LMDBDataInterfaceFactory(ApplicationContext context) {
        super(context);
        directory = context.getProperty("data_directory");
        env = new Env();
        env.setMaxDbs(100);
        env.setMapSize(200 * 1024 * 1024); //needs to be quite high, otherwise we get EINVAL or MDB_MAP_FULL errors
        File directoryAsFile = new File(directory);
        if (directoryAsFile.isFile()) {
            throw new RuntimeException("Path " + directoryAsFile.getAbsolutePath() + " is a file! Expected a directory...");
        } else if (!directoryAsFile.exists()) {
            boolean success = directoryAsFile.mkdirs();
            if (!success) {
                throw new RuntimeException("Failed to create directory " + directoryAsFile.getAbsolutePath());
            }
        }
        env.open(directory);
    }

    @Override
    protected <T> BaseDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporaryDataInterface) {
        return new LMDBDataInterface<>(name, objectClass, combinator, objectSerializer, env, isTemporaryDataInterface);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return LMDBDataInterface.class;
    }

    @Override
    public synchronized void terminate() {
        super.terminate();
        env.close();
    }
}
