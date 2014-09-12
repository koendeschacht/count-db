package be.bow.db.data;

import be.bow.db.Combinator;

public class IndexCombinator implements Combinator<Index> {
    @Override
    public Index combine(Index first, Index second) {
        Index result = new Index();
        result.addAll(first);
        result.addAll(second);
        return result;
    }
}
