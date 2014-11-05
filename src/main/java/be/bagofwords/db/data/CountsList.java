package be.bagofwords.db.data;

import be.bagofwords.util.ByteArraySerializable;
import be.bagofwords.util.HashUtils;
import be.bagofwords.util.Pair;
import be.bagofwords.util.SerializationUtils;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class CountsList extends ArrayList<Pair<Long, Long>> implements ByteArraySerializable {

    public CountsList() {
        super();
    }

    public CountsList(CountsList list) {
        super(list);
    }

    public CountsList(long key, int count) {
        this();
        addCount(key, count);
    }

    public CountsList(int initialSize) {
        super(initialSize);
    }

    public CountsList(byte[] serialized) {
        int longWidth = SerializationUtils.getWidth(Long.class);
        int size = serialized.length / (longWidth * 2);
        int offset = 0;
        for (int i = 0; i < size; i++) {
            long key = SerializationUtils.bytesToLong(serialized, offset);
            long value = SerializationUtils.bytesToLong(serialized, serialized.length / 2 + offset);
            offset += longWidth;
            add(new Pair<>(key, value));
        }
    }

    public synchronized long getCount(long key) {
        int ind = Collections.binarySearch((List) this, key);
        if (ind >= 0) {
            return get(ind).getSecond();
        } else {
            return 0;
        }
    }

    public synchronized void addCount(long key, long count) {
        add(new Pair<>(key, count));
    }

    @JsonIgnore
    public synchronized long getTotal() {
        long total = 0;
        for (Pair<Long, Long> value : this) {
            total += value.getSecond();
        }
        return total;
    }

    public synchronized CountsList clone() {
        return new CountsList(this);
    }

    public void compact() {
        if (!isCompacted()) {
            synchronized (this) {
                if (!isCompacted()) {
                    List<Pair<Long, Long>> oldCounts = new ArrayList<>(this);
                    clear();
                    Collections.sort(oldCounts);
                    for (int i = 0; i < oldCounts.size(); ) {
                        Pair<Long, Long> curr = oldCounts.get(i);
                        long combinedCount = curr.getSecond();
                        i++;
                        while (i < oldCounts.size() && oldCounts.get(i).getFirst().equals(curr.getFirst())) {
                            combinedCount += oldCounts.get(i++).getSecond();
                        }
                        add(new Pair<>(curr.getFirst(), combinedCount));
                    }
                }
            }
        }
    }

    private boolean isCompacted() {
        for (int i = 1; i < size(); i++) {
            if (get(i - 1).getFirst() >= get(i).getFirst()) {
                return false;
            }
        }
        return true;
    }

    public synchronized int hashCode() {
        int result = HashUtils.startHash;
        for (Pair<Long, Long> value : this) {
            result = result * HashUtils.addHash + value.getFirst().intValue();
            result = result * HashUtils.addHash + value.getSecond().hashCode();
        }
        return result;
    }

    public String toString() {
        String result = "{ ";
        int i;
        for (i = 0; i < size() && i < 10; i++) {
            result += get(i).getFirst() + ":" + get(i).getSecond();
            if (i < size() - 1) {
                result += ", ";
            }
        }
        if (i < size()) {
            result += " ...";
        }
        result += "}";
        return result;
    }

    public List<Long> getSortedKeys() {
        List<Pair<Long, Long>> sortedValues = new ArrayList<>(this);
        Collections.sort(sortedValues, new Comparator<Pair<Long, Long>>() {
            @Override
            public int compare(Pair<Long, Long> val1, Pair<Long, Long> val2) {
                if (val1.getSecond().equals(val2.getSecond())) {
                    return Long.compare(val1.getFirst(), val2.getFirst());
                } else {
                    return -Long.compare(val1.getSecond(), val2.getSecond()); //largest first
                }
            }
        });
        List<Long> result = new ArrayList<>();
        for (Pair<Long, Long> value : sortedValues) {
            result.add(value.getFirst());
        }
        return result;
    }

    @Override
    public byte[] toByteArray() {
        int longWidth = SerializationUtils.getWidth(Long.class);
        int size = size() * longWidth * 2;
        byte[] result = new byte[size];
        int offset = 0;
        for (Pair<Long, Long> value : this) {
            SerializationUtils.longToBytes(value.getFirst(), result, offset);
            offset += longWidth;
        }
        for (Pair<Long, Long> value : this) {
            SerializationUtils.longToBytes(value.getSecond(), result, offset);
            offset += longWidth;
        }
        if (offset != result.length) {
            throw new RuntimeException("Something went wrong!");
        }
        return result;
    }
}
