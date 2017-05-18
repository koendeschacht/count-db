package be.bagofwords.db.remote;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.minidepi.ApplicationContext;

public class RemoteDatabaseInterfaceFactory extends DataInterfaceFactory {

    private final String host;
    private final int port;

    public RemoteDatabaseInterfaceFactory(ApplicationContext context) {
        super(context);
        this.host = context.getProperty("socket.host", "count-db.properties");
        this.port = Integer.parseInt(context.getProperty("socket.port", "count-db.properties"));
    }

    @Override
    public synchronized <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new RemoteDataInterface<>(nameOfSubset, objectClass, combinator, host, port, isTemporaryDataInterface, taskScheduler);
    }

}
