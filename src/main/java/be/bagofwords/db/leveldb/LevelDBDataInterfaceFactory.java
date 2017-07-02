package be.bagofwords.db.leveldb;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.minidepi.ApplicationContext;

import java.io.File;

public class LevelDBDataInterfaceFactory extends BaseDataInterfaceFactory {

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
    protected <T extends Object> BaseDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporaryDataInterface) {
        return new LevelDBDataInterface<>(directory, name, objectClass, combinator, objectSerializer, isTemporaryDataInterface);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return LevelDBDataInterface.class;
    }

}
