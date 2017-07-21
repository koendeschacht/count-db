package be.bagofwords.db.experimental.index;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.impl.UpdateListener;

/**
 * Created by koen on 16/07/17.
 */
public abstract class BaseDataInterfaceIndex<T> implements UpdateListener<T> {

    protected final DataInterface<T> dataInterface;

    protected BaseDataInterfaceIndex(DataInterface<T> dataInterface) {
        this.dataInterface = dataInterface;
        this.dataInterface.registerUpdateListener(this);
    }

    protected abstract String getIndexName();

}
