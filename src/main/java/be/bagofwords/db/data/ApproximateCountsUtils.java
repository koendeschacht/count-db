package be.bagofwords.db.data;

import be.bagofwords.db.bloomfilter.LongBloomFilter;

public class ApproximateCountsUtils {

    public static ApproximateCountsFilter createEmptyCountsFilter() {
        LongCountsBloomFilter bloomFilter = new LongCountsBloomFilter(1, 0.1);
        long[] averages = new long[0];
        return new ApproximateCountsFilter(averages, createEmptyBloomFilter(), bloomFilter);
    }

    public static LongBloomFilter createEmptyBloomFilter() {
        return new LongBloomFilter(1, 0.1);
    }

    public static LongBloomFilter mergeBloomFilters(LongBloomFilter first, LongBloomFilter second) {
        if (first.getNumOfHashFunctions() != second.getNumOfHashFunctions()) {
            throw new RuntimeException("Unequal number of hash functions!");
        }
        LongBloomFilter.BitArray bitArray1 = first.getBits();
        LongBloomFilter.BitArray bitArray2 = second.getBits();
        return new LongBloomFilter(bitArray1.mergeWith(bitArray2), first.getNumOfHashFunctions());
    }

    public static LongCountsBloomFilter mergeBloomCountFilters(LongCountsBloomFilter first, LongCountsBloomFilter second) {
        if (first.getNumOfHashFunctions() != second.getNumOfHashFunctions()) {
            throw new RuntimeException("Unequal number of hash functions!");
        }
        LongCountsBloomFilter.ByteArray byteArray1 = first.getBytes();
        LongCountsBloomFilter.ByteArray byteArray2 = second.getBytes();
        return new LongCountsBloomFilter(byteArray1.mergeWith(byteArray2), first.getNumOfHashFunctions());
    }
}
