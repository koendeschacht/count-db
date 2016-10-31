package be.bagofwords.db.remote;

import be.bagofwords.application.ApplicationContext;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactory;
import be.bagofwords.db.combinator.Combinator;

public class RemoteDatabaseInterfaceFactory extends DataInterfaceFactory {

    private final String host;
    private final int port;

    public RemoteDatabaseInterfaceFactory(ApplicationContext context) {
        super(context);
        this.host = context.getConfig("remote_interface_host", "localhost");
        this.port = Integer.parseInt(context.getConfig("remote_interface_port", "1208"));
    }

    @Override
    public synchronized <T extends Object> DataInterface<T> createBaseDataInterface(String nameOfSubset, Class<T> objectClass, Combinator<T> combinator, boolean isTemporaryDataInterface) {
        return new RemoteDataInterface<>(nameOfSubset, objectClass, combinator, host, port, isTemporaryDataInterface, taskScheduler);
    }

}
