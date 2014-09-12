package be.bow.db.data;

import java.util.ArrayList;

/**
 * List of unique values *
 */

public class LongList extends ArrayList<Long> {

    public LongList() {
    }

    public LongList(Long firstValue) {
        super();
        add(firstValue);
    }
}
