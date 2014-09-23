package be.bagofwords.db;

import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SerializationUtils;

public class DataInterfaceUtils {

    public static <T> void listDataItems(DataInterface<T> dataInterface) {
        CloseableIterator<KeyValue<T>> it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<T> next = it.next();
            UI.write(next.getKey() + " ");
            UI.write(SerializationUtils.objectToString(next.getValue(), true));
        }
        it.close();
        UI.write("Closing connection");
    }

}
