package be.bagofwords.db;

/**
 * Created by koen on 20/05/17.
 */
public interface KeyFilter {

    default boolean acceptKey(long key) {
        return acceptKeysBelow(key + 1) && acceptKeysAboveOrEqual(key);
    }

    default boolean acceptKeysBelow(long key) {
        return true;
    }

    default boolean acceptKeysAboveOrEqual(long key) {
        return true;
    }

}
