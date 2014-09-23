package be.bagofwords.db.data;

import be.bagofwords.util.Pair;

import java.util.ArrayList;

public class Index extends ArrayList<Pair<Long, Integer>> {

    public Index() {
        super();
    }

    public Index(long index, int offset) {
        super();
        add(new Pair<>(index, offset));
    }
}
