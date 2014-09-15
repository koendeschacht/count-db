package be.bow.db;

import be.bow.db.bloomfilter.LongBloomFilter;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestBloomFilter {

    @Test
    public void testBloomFilterHitRates() {
        int numOfExamples = 10000000;
        LongBloomFilter bloomFilter = new LongBloomFilter(numOfExamples, 0.05);
        Random random = new Random(1204);
        List<Long> posExamples = new ArrayList<>();
        for (int i = 0; i < numOfExamples; i++) {
            long randomVal = random.nextLong();
            bloomFilter.put(randomVal);
            posExamples.add(randomVal);
        }
        for (int i = 0; i < numOfExamples; i++) {
            Assert.assertTrue(bloomFilter.mightContain(posExamples.get(i)));
        }
        int overClass = 0;
        for (int i = 0; i < numOfExamples; i++) {
            long randomVal = random.nextLong();
            if (bloomFilter.mightContain(randomVal)) {
                overClass++;
            }
        }
        double actualFpp = overClass / (double) numOfExamples;
        Assert.assertTrue(actualFpp < 0.1);
    }

    @Test
    public void testBloomFilterLowNumbers() {
        int numOfExamples = 1000000;
        LongBloomFilter bloomFilter = new LongBloomFilter(numOfExamples, 0.05);
        for (int i = 0; i < numOfExamples; i++) {
            bloomFilter.put(i);
        }
        for (int i = 0; i < numOfExamples; i++) {
            Assert.assertTrue(bloomFilter.mightContain(i));
        }
        int overClass = 0;
        for (int i = numOfExamples; i < numOfExamples * 2; i++) {
            if (bloomFilter.mightContain(i)) {
                overClass++;
            }
        }
        double actualFpp = overClass / (double) numOfExamples;
        Assert.assertTrue(actualFpp < 0.1);
    }
}
