package be.bow.db;

import be.bow.iterator.CloseableIterator;
import be.bow.ui.UI;
import be.bow.util.KeyValue;
import be.bow.util.SerializationUtils;

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
