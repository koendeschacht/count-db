package be.bagofwords.db.leveldb;

import be.bagofwords.db.impl.DataInterfaceFactoryImpl;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.minidepi.ApplicationContext;

import java.io.File;

public class LevelDBDataInterfaceFactory extends DataInterfaceFactoryImpl {

    private final String directory;

    public LevelDBDataInterfaceFactory(ApplicationContext context) {
        super(context);
        this.directory = context.getProperty("data_directory");
        File dirFile = new File(directory);
        if (!dirFile.exists()) {
            dirFile.mkdirs();
        }
    }

    @Override
    protected <T extends Object> BaseDataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new LevelDBDataInterface<>(directory, nameOfSubset, objectClass, combinator, isTemporaryDataInterface, getMetaDataStore());
    }

}
