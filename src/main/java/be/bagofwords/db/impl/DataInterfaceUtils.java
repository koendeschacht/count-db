package be.bagofwords.db.impl;

import be.bagofwords.db.DataInterface;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;

public class DataInterfaceUtils {

    public static <T> void listDataItems(DataInterface<T> dataInterface) {
        CloseableIterator<KeyValue<T>> it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<T> next = it.next();
            Log.i(next.getKey() + " ");
            Log.i(SerializationUtils.serializeObject(next.getValue(), true));
        }
        it.close();
        Log.i("Closing connection");
    }

}
