package be.bagofwords.db.combinator;

import be.bagofwords.db.data.DoubleCountsList;

import java.io.Serializable;

public class DoubleCountsListCombinator implements Combinator<DoubleCountsList>, Serializable {

    @Override
    public DoubleCountsList combine(DoubleCountsList first, DoubleCountsList second) {
        DoubleCountsList result = new DoubleCountsList(first);
        result.addAll(second);
        result.compact();
        return result;
    }

}
