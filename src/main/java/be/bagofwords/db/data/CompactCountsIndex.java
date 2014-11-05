package be.bagofwords.db.data;

import be.bagofwords.util.Pair;
import org.codehaus.jackson.annotate.JsonIgnore;

public class CompactCountsIndex {

    public static final double FPP = 0.01;

    private int numberOfCounts;
    private long maxCounts;
    private CountsList cachedKeys;
    private LongCountsBloomFilter filterCounts;

    public CompactCountsIndex(long maxCounts, int numberOfCounts) {
        this.maxCounts = maxCounts;
        this.numberOfCounts = numberOfCounts;
        this.cachedKeys = new CountsList();
    }

    public CompactCountsIndex(long maxCounts, LongCountsBloomFilter filterCounts, int numberOfCounts) {
        this.maxCounts = maxCounts;
        this.numberOfCounts = numberOfCounts;
        this.filterCounts = filterCounts;
    }

    public CompactCountsIndex() {

    }

    @JsonIgnore
    public boolean isSparse() {
        return filterCounts == null;
    }

    public void addCount(long key) {
        addCount(key, 1);
    }

    //Serialization

    public void addCount(long key, int count) {
        if (isSparse()) {
            cachedKeys.addCount(key, count);
        } else {
            filterCounts.addCount(key, count);
        }
        numberOfCounts += count;
    }

    public long getMaxCounts() {
        return maxCounts;
    }

    public void setMaxCounts(long maxCounts) {
        this.maxCounts = maxCounts;
    }

    public CountsList getCachedKeys() {
        return cachedKeys;
    }

    public void setCachedKeys(CountsList cachedKeys) {
        this.cachedKeys = cachedKeys;
    }

    public LongCountsBloomFilter getFilterCounts() {
        return filterCounts;
    }

    public void setFilterCounts(LongCountsBloomFilter filterCounts) {
        this.filterCounts = filterCounts;
    }

    public int getNumberOfCounts() {
        return numberOfCounts;
    }

    public void setNumberOfCounts(int numberOfCounts) {
        this.numberOfCounts = numberOfCounts;
    }

    public int getCount(long key) {
        if (isSparse()) {
            return (int) cachedKeys.getCount(key);
        } else {
            return filterCounts.getMaxCount(key);
        }
    }

    public void incrementNumberOfCounts() {
        numberOfCounts++;
    }

    public CompactCountsIndex mergeWith(CompactCountsIndex second) {
        CompactCountsIndex result;
        long maxSizeForSparse = getNumberOfValuesForSparse();
        boolean makeSparse = this.isSparse() && second.isSparse() && this.getCachedKeys().size() + second.getCachedKeys().size() < maxSizeForSparse;
        if (makeSparse) {
            result = new CompactCountsIndex(this.getMaxCounts(), this.getNumberOfCounts() + second.getNumberOfCounts());
            result.setCachedKeys(new CountsList(this.getCachedKeys()));
            for (Pair<Long, Long> value : second.getCachedKeys()) {
                result.addCount(value.getFirst(), value.getSecond().intValue());
            }
        } else {
            LongCountsBloomFilter mergedCounts;
            if (!this.isSparse() && !second.isSparse()) {
                mergedCounts = ApproximateCountsUtils.mergeBloomCountFilters(this.getFilterCounts(), second.getFilterCounts());
            } else {
                if (!this.isSparse()) {
                    mergedCounts = this.getFilterCounts().clone();
                } else if (!second.isSparse()) {
                    mergedCounts = second.getFilterCounts().clone();
                } else {
                    //Both sparse
                    mergedCounts = new LongCountsBloomFilter(this.getMaxCounts(), CompactCountsIndex.FPP);
                }
            }
            if (this.isSparse()) {
                for (Pair<Long, Long> value : this.getCachedKeys()) {
                    mergedCounts.addCount(value.getFirst(), value.getSecond().intValue());
                }
            }
            if (second.isSparse()) {
                for (Pair<Long, Long> value : second.getCachedKeys()) {
                    mergedCounts.addCount(value.getFirst(), value.getSecond().intValue());
                }
            }
            result = new CompactCountsIndex(this.getMaxCounts(), mergedCounts, this.getNumberOfCounts() + second.getNumberOfCounts());
        }
        result.compact();
        return result;
    }

    public void compact() {
        if (isSparse()) {
            getCachedKeys().compact();
        }
    }

    @JsonIgnore
    public int getNumberOfValuesForSparse() {
        return (int) this.getMaxCounts() / 10;
    }

    @JsonIgnore
    public int getTotal() {
        return getNumberOfCounts();
    }

    public CompactCountsIndex clone() {
        CompactCountsIndex clone = new CompactCountsIndex(getMaxCounts(), getNumberOfCounts());
        if (isSparse()) {
            clone.setCachedKeys(getCachedKeys().clone());
        } else {
            clone.setFilterCounts(getFilterCounts().clone());
        }
        return clone;
    }
}
