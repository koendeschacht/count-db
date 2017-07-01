package be.bagofwords.db.remote;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.impl.BaseDataInterfaceFactory;
import be.bagofwords.db.impl.BaseDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.minidepi.ApplicationContext;

public class RemoteDatabaseInterfaceFactory extends BaseDataInterfaceFactory {

    private final String host;
    private final int port;

    public RemoteDatabaseInterfaceFactory(ApplicationContext context) {
        super(context);
        this.host = context.getProperty("socket.host", "count-db.properties");
        this.port = Integer.parseInt(context.getProperty("socket.port", "count-db.properties"));
    }

    @Override
    protected synchronized <T extends Object> BaseDataInterface<T> createBaseDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new RemoteDataInterface<>(name, objectClass, combinator, host, port, isTemporaryDataInterface, asyncJobService);
    }

    @Override
    protected Class<? extends DataInterface> getBaseDataInterfaceClass() {
        return RemoteDataInterface.class;
    }

}
