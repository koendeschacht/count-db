package be.bagofwords.db.methods;

import be.bagofwords.exec.RemoteClass;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by koen on 20/05/17.
 */
@RemoteClass
public class SetKeyFilter implements KeyFilter, Serializable {

    private Set<Long> keys;

    public SetKeyFilter(Set<Long> keys) {
        this.keys = keys;
    }

    @Override
    public boolean acceptKey(long key) {
        return keys.contains(key);
    }

    @Override
    public boolean acceptKeysBelow(long key) {
        return true;
    }

    @Override
    public boolean acceptKeysAboveOrEqual(long key) {
        return true;
    }
}
