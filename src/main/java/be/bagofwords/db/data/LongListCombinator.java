package be.bagofwords.db.data;

import be.bagofwords.db.combinator.Combinator;

import java.util.Collections;

public class LongListCombinator implements Combinator<LongList> {

    @Override
    public LongList combine(LongList first, LongList second) {
        LongList result = new LongList();
        result.addAll(first);
        result.addAll(second);
        Collections.sort(result);
        for (int i = 1; i < result.size(); i++) {
            if (result.get(i - 1).equals(result.get(i))) {
                result.remove(i);
                i--;
            }
        }
        return result;
    }

}
