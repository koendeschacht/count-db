package be.bagofwords.db.speedy;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.filedb.FileDataInterface;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.annotations.Inject;

/**
 * Created by koen on 29/05/17.
 */
public class SpeedyDataInterfaceFactory extends BaseDataInterfaceFactory {

    private String directory;

    @Inject
    public SpeedyDataInterfaceFactory(ApplicationContext context) {
        super(context);
        this.directory = context.getProperty("data_directory");
    }

    @Override
    protected <T> BaseDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporaryDataInterface) {
        if(objectClass!=Long.class) {
            return new FileDataInterface<>(memoryManager, combinator, objectClass, this.directory, name, isTemporaryDataInterface, asyncJobService, objectSerializer);
        }
        return new SpeedyDataInterface<>(name, objectClass, combinator, objectSerializer, isTemporaryDataInterface, directory);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return SpeedyDataInterface.class;
    }
}
