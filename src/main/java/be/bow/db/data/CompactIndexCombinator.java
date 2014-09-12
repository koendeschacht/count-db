package be.bow.db.data;

import be.bow.db.combinator.Combinator;

public class CompactIndexCombinator implements Combinator<CompactIndex> {

    @Override
    public CompactIndex combine(CompactIndex first, CompactIndex second) {
        return first.mergeWith(second);
    }
}
