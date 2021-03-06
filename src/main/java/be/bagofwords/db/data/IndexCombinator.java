package be.bagofwords.db.data;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.exec.RemoteClass;

@RemoteClass
public class IndexCombinator implements Combinator<Index> {
    @Override
    public Index combine(Index first, Index second) {
        Index result = new Index();
        result.addAll(first);
        result.addAll(second);
        return result;
    }
}
