package be.bow.db.bloomfilter;

import be.bow.db.DataInterface;
import be.bow.db.LayeredDataInterface;
import be.bow.iterator.CloseableIterator;

import java.util.concurrent.locks.ReentrantLock;

public class BloomFilterDataInterface<T extends Object> extends LayeredDataInterface<T> {

    private static final double INITIAL_FPP = 0.001;
    private final static double MAX_FPP = INITIAL_FPP * 20;
    private final DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface;
    private final ReentrantLock modifyBloomFilterLock;
    private LongBloomFilterWithCheckSum bloomFilter;
    private boolean bloomFilterWasWrittenToDisk;
    private long currentKeyForNewBloomFilterCreation = Long.MAX_VALUE;

    private long timeOfLastRead;

    public BloomFilterDataInterface(DataInterface<T> baseInterface, DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface) {
        super(baseInterface);
        this.bloomFilterDataInterface = bloomFilterDataInterface;
        this.modifyBloomFilterLock = new ReentrantLock();
        this.bloomFilter = bloomFilterDataInterface.read(getName());
    }

    @Override
    public void optimizeForReading() {
        if (bloomFilter == null) {
            createNewBloomFilter();
        }
        baseInterface.optimizeForReading();
    }

    @Override
    public T readInt(long key) {
        timeOfLastRead = System.currentTimeMillis();
        LongBloomFilterWithCheckSum currentBloomFilter = bloomFilter;
        if (currentBloomFilter == null && modifyBloomFilterLock.tryLock()) {
            createNewBloomFilter();
            currentBloomFilter = bloomFilter;
            modifyBloomFilterLock.unlock();
        }
        if (currentBloomFilter == null || currentKeyForNewBloomFilterCreation < key) {
            //we are still creating the bloom filter
            return baseInterface.read(key);
        } else {
            if (currentBloomFilter.mightContain(key)) {
                return baseInterface.read(key);
            } else {
                return null;
            }
        }
    }


    @Override
    public void dropAllData() {
        modifyBloomFilterLock.lock();
        try {
            baseInterface.dropAllData();
            createNewBloomFilterNonSynchronized();
        } finally {
            modifyBloomFilterLock.unlock();
        }
    }

    @Override
    public boolean mightContain(long key) {
        LongBloomFilterWithCheckSum currentBloomFilter = bloomFilter;
        if (currentBloomFilter == null && modifyBloomFilterLock.tryLock()) {
            createNewBloomFilter();
            currentBloomFilter = bloomFilter;
            modifyBloomFilterLock.unlock();
        }
        if (currentBloomFilter == null || currentKeyForNewBloomFilterCreation < key) {
            //we are still creating the bloom filter
            return baseInterface.mightContain(key);
        } else {
            return currentBloomFilter.mightContain(key);
        }
    }

    @Override
    public void valuesChanged(long[] keys) {
        super.valuesChanged(keys);
        modifyBloomFilterLock.lock();
        if (bloomFilter != null) {
            if (keys.length > 0) {
                bloomFilterWasWrittenToDisk = false;
            }
            for (Long key : keys) {
                bloomFilter.put(key);
            }
            if (bloomFilter.expectedFpp() > MAX_FPP) {
                if (System.currentTimeMillis() - timeOfLastRead < 1000) {
                    //read in last second, create a new bloom filter
                    createNewBloomFilterNonSynchronized();
                } else {
                    //no reads in last second, let's drop the bloom filter for now
                    bloomFilter = null;
                }
            }
        }
        modifyBloomFilterLock.unlock();
    }

    private void createNewBloomFilterNonSynchronized() {
        currentKeyForNewBloomFilterCreation = Long.MIN_VALUE;
        long numOfValuesForBloomFilter = Math.max(1000000, baseInterface.apprSize());
        bloomFilter = new LongBloomFilterWithCheckSum(numOfValuesForBloomFilter, INITIAL_FPP);
        bloomFilter.setDataCheckSum(baseInterface.dataCheckSum());
        long start = System.currentTimeMillis();
        int numOfKeys = 0;
        CloseableIterator<Long> it = baseInterface.keyIterator();
        while (it.hasNext()) {
            long key = it.next();
            bloomFilter.put(key);
            numOfKeys++;
            currentKeyForNewBloomFilterCreation = key;
        }
        it.close();
        currentKeyForNewBloomFilterCreation = Long.MAX_VALUE;
        long taken = (System.currentTimeMillis() - start);
//        UI.write("Created bloomfilter " + getName() + " in " + taken + " ms for " + numOfKeys + " keys, size is " + bloomFilter.getBits().size() / (8 * 1024) + " kbytes.");
    }

    private void createNewBloomFilter() {
        modifyBloomFilterLock.lock();
        createNewBloomFilterNonSynchronized();
        modifyBloomFilterLock.unlock();
    }

    @Override
    public void flush() {
        baseInterface.flush();
        if (!bloomFilterWasWrittenToDisk) {
            bloomFilterDataInterface.write(getName(), bloomFilter);
            bloomFilterWasWrittenToDisk = true;
        }
    }
}
