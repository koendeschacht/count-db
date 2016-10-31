package be.bagofwords.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by koen on 31.10.16.
 */
public class IdObjectList<S, T extends IdObject<S>> extends ArrayList<T> {

    public IdObjectList() {
        super();
    }

    public IdObjectList(Collection<? extends T> c) {
        super(c);
    }

    public IdObjectList(T object) {
        super(Collections.singleton(object));
    }
}
