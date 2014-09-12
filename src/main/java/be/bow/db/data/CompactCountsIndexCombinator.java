package be.bow.db.data;

import be.bow.db.Combinator;

public class CompactCountsIndexCombinator implements Combinator<CompactCountsIndex> {

    @Override
    public CompactCountsIndex combine(CompactCountsIndex first, CompactCountsIndex second) {
        return first.mergeWith(second);
    }
}
