package be.bow.db.data;


import be.bow.db.bloomfilter.LongBloomFilter;

public class ApproximateCountsFilter {

    private long[] averageValues;
    private LongCountsBloomFilter otherCountsBloomFilter;
    private LongBloomFilter oneCountsBloomFilter;

    public ApproximateCountsFilter(long[] averageValues, LongBloomFilter onceCountsBloomFilter, LongCountsBloomFilter otherCountsBloomFilter) {
        this.averageValues = averageValues;
        this.oneCountsBloomFilter = onceCountsBloomFilter;
        this.otherCountsBloomFilter = otherCountsBloomFilter;
    }

    public long getCount(long key) {
        int ind = otherCountsBloomFilter.getMaxCount(key);
        if (ind == 0) {
            if (oneCountsBloomFilter.mightContain(key)) {
                return 1;
            } else {
                return 0;
            }
        } else {
            return averageValues[ind - 1];
        }
    }

    public long[] getAverageValues() {
        return averageValues;
    }

    public LongCountsBloomFilter getOtherCountsBloomFilter() {
        return otherCountsBloomFilter;
    }

    public LongBloomFilter getOneCountsBloomFilter() {
        return oneCountsBloomFilter;
    }

    //Serialization

    public ApproximateCountsFilter() {
    }

    public void setAverageValues(long[] averageValues) {
        this.averageValues = averageValues;
    }

    public void setOtherCountsBloomFilter(LongCountsBloomFilter otherCountsBloomFilter) {
        this.otherCountsBloomFilter = otherCountsBloomFilter;
    }

    public void setOneCountsBloomFilter(LongBloomFilter oneCountsBloomFilter) {
        this.oneCountsBloomFilter = oneCountsBloomFilter;
    }
}
