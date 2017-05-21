package be.bagofwords.db.helper;

import be.bagofwords.db.KeyFilter;
import be.bagofwords.exec.RemoteClass;

import java.io.Serializable;

/**
 * Created by koen on 21/05/17.
 */
@RemoteClass
public class EvenKeysFilter implements KeyFilter, Serializable {

    @Override
    public boolean acceptKey(long key) {
        return key % 2 == 0;
    }

}
