package be.bow.db.data;

import be.bow.counts.BinComputer;
import be.bow.iterator.CloseableIterator;
import be.bow.ui.UI;
import be.bow.util.KeyValue;
import be.bow.util.NumUtils;
import be.bow.db.DataInterface;
import be.bow.db.bloomfilter.LongBloomFilter;

import java.util.ArrayList;
import java.util.List;

public class ApproximateCountsUtils {

    public static ApproximateCountsFilter createFilterFromDataInterface(DataInterface<Long> dataInterface, double fpp) {
        int numOfValuesForBins = 10000;
        BinComputer bc = new BinComputer(numOfValuesForBins);
        CloseableIterator<KeyValue<Long>> it = dataInterface.iterator();
        while (it.hasNext() && bc.getAllValues().size() < numOfValuesForBins) {
            KeyValue<Long> next = it.next();
            if (next.getValue() > 1) {
                bc.addCount(next.getValue());
            }
        }
        it.close();
        double[] binBorders = bc.getEquiDenseBins(256 - 2);
        List<Long>[] binnedValues = new List[binBorders.length + 1];
        for (int i = 0; i < binnedValues.length; i++) {
            binnedValues[i] = new ArrayList<>();
        }
        for (double value : bc.getAllValues()) {
            int bin = NumUtils.getBin(binBorders, value);
            binnedValues[bin].add(Math.round(value));
        }
        long[] averageValues = new long[binnedValues.length];
        for (int i = 0; i < binnedValues.length; i++) {
            if (!binnedValues[i].isEmpty()) {
                double average = NumUtils.sumOfLongValues(binnedValues[i]) / (double) binnedValues[i].size();
                averageValues[i] = Math.round(average);
            } else {
                if (i > 0) {
                    averageValues[i] = (long) binBorders[i - 1] + 1;
                } else {
                    averageValues[i] = 1;
                }
            }
        }
        long[] numOfExpectedValues = countValues(dataInterface);
        LongBloomFilter oneCountsBloomFilter = new LongBloomFilter(numOfExpectedValues[0], fpp);
        LongCountsBloomFilter otherCountsBloomFilter = new LongCountsBloomFilter(numOfExpectedValues[1], fpp);
        UI.write("Adding all counts to the filter with size " + otherCountsBloomFilter.getBytes().size() + "+" + (oneCountsBloomFilter.getBits().size() / 8) + ". This might take a while");
        it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<Long> value = it.next();
            if (value.getValue() == 1) {
                oneCountsBloomFilter.put(value.getKey());
            } else {
                int bin = NumUtils.getBin(binBorders, value.getValue());
                otherCountsBloomFilter.addCount(value.getKey(), bin + 1);
            }
        }
        it.close();
        return new ApproximateCountsFilter(averageValues, oneCountsBloomFilter, otherCountsBloomFilter);
    }

    private static long[] countValues(DataInterface<Long> dataInterface) {
        long oneCounts = 0;
        long otherCounts = 0;
        CloseableIterator<KeyValue<Long>> it = dataInterface.iterator();
        while (it.hasNext()) {
            KeyValue<Long> value = it.next();
            if (value.getValue() == 1) {
                oneCounts++;
            } else {
                otherCounts++;
            }
        }
        it.close();
        return new long[]{oneCounts, otherCounts};
    }

    public static ApproximateCountsFilter createEmptyCountsFilter() {
        LongCountsBloomFilter bloomFilter = new LongCountsBloomFilter(1, 0.1);
        long[] averages = new long[0];
        return new ApproximateCountsFilter(averages, createEmptyBloomFilter(), bloomFilter);
    }

    public static LongBloomFilter createEmptyBloomFilter() {
        return new LongBloomFilter(1, 0.1);
    }

    public static LongBloomFilter createTotalBloomFilter(DataInterface<Long> totalCountsDI, int minFrequency) {
        long start = System.currentTimeMillis();
        long numOfValuesToAdd = 0;
        CloseableIterator<KeyValue<Long>> it = totalCountsDI.iterator();
        while (it.hasNext()) {
            if (it.next().getValue() >= minFrequency) {
                numOfValuesToAdd++;
            }
        }
        it.close();
        LongBloomFilter result = new LongBloomFilter(numOfValuesToAdd, 0.01);
        it = totalCountsDI.iterator();
        while (it.hasNext()) {
            KeyValue<Long> next = it.next();
            if (next.getValue() >= minFrequency) {
                result.put(next.getKey());
            }
        }
        it.close();
        UI.write("Creating bloom filter took " + (System.currentTimeMillis() - start) + " ms for " + numOfValuesToAdd + " values, taking " + (result.getBits().getData().length * 8) + " bytes");
        return result;
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
