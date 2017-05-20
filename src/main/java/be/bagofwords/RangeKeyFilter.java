package be.bagofwords;

import be.bagofwords.db.KeyFilter;

/**
 * Created by koen on 20/05/17.
 */
public class RangeKeyFilter implements KeyFilter {

    private long lowerBound;
    private long higherBound;

    @Override
    public boolean acceptKey(long key) {
        return key >= lowerBound && key < higherBound;
    }

    @Override
    public boolean acceptKeysBelow(long key) {
        return key < higherBound;
    }

    @Override
    public boolean acceptKeysAboveOrEqual(long key) {
        return key >= lowerBound;
    }
}
