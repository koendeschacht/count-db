package be.bagofwords.sim;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.data.CountsList;

import java.io.Serializable;

public class CountsListCombinator implements Combinator<CountsList>, Serializable {

    @Override
    public CountsList combine(CountsList first, CountsList second) {
        CountsList result = new CountsList(first);
        result.addAll(second);
        result.compact();
        return result;
    }

}

