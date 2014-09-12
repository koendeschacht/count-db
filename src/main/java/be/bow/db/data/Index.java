package be.bow.db.data;

import be.bow.util.Pair;

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
