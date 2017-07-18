package be.bagofwords.db.experimental.index;

import be.bagofwords.db.combinator.Combinator;

/**
 * Created by koen on 16/07/17.
 */
public class UniqueKeyCombinator implements Combinator<Long> {

    @Override
    public Long combine(Long first, Long second) {
        if (first == null) {
            return second;
        } else if (second == null) {
            return first;
        } else if (first.equals(second)) {
            return first;
        } else {
            throw new RuntimeException("Trying to combine two keys (" + first + " and " + second + ") for same index key ");
        }
    }
}
