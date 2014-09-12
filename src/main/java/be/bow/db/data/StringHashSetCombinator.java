package be.bow.db.data;

import be.bow.db.Combinator;

public class StringHashSetCombinator implements Combinator<StringHashSet> {

    @Override
    public StringHashSet combine(StringHashSet first, StringHashSet second) {
        StringHashSet result = new StringHashSet();
        result.addAll(first);
        result.addAll(second);
        return result;
    }

}
