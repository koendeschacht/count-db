package be.bow.db.bloomfilter;

import be.bow.db.DataInterface;
import be.bow.db.LayeredDataInterface;
import be.bow.iterator.CloseableIterator;

import java.util.concurrent.locks.ReentrantLock;

public class BloomFilterDataInterface<T extends Object> extends LayeredDataInterface<T> {

    private static final double INITIAL_FPP = 0.01;
    private final static double MAX_FPP = INITIAL_FPP * 2;
    private final DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface;
    private LongBloomFilterWithCheckSum bloomFilter;
    private ReentrantLock modifyBloomFilterLock;
    private long currentKeyForNewBloomFilterCreation = Long.MAX_VALUE;

    public BloomFilterDataInterface(DataInterface<T> baseInterface, DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface) {
        super(baseInterface);
        this.bloomFilterDataInterface = bloomFilterDataInterface;
        this.modifyBloomFilterLock = new ReentrantLock();
    }

    private void createNewBloomFilter() {
        baseInterface.flush(); //Make sure all values are written to disk
        long numOfValuesForBloomFilter = Math.max(1000, baseInterface.apprSize());
//        UI.write("Creating bloomfilter " + getName());
        currentKeyForNewBloomFilterCreation = Long.MIN_VALUE;
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
//        long taken = (System.currentTimeMillis() - start);
//        UI.write("Created bloomfilter " + getName() + " in " + taken + " ms for " + numOfKeys + " keys.");
    }

    @Override
    public void optimizeForReading() {
        checkInitialization(); //create bloom filter if necessary
        baseInterface.optimizeForReading();
    }

    @Override
    public T readInt(long key) {
        checkInitialization();
        if (bloomFilter == null || currentKeyForNewBloomFilterCreation < key) {
            //we are still creating the bloom filter
            return baseInterface.read(key);
        } else {
            boolean mightContainValue = true;
            try {
                mightContainValue = bloomFilter.mightContain(key);
            } catch (NullPointerException exp) {
                //bloom filter might have become null. We don't use a lock to prevent that case since that slows things down too much.
            }
            if (mightContainValue) {
                return baseInterface.read(key);
            } else {
                return null;
            }
        }
    }

    private void checkInitialization() {
        if (bloomFilter == null) {
            if (modifyBloomFilterLock.tryLock()) {
                bloomFilter = bloomFilterDataInterface.read(getName());
                if (bloomFilter == null || bloomFilter.getDataCheckSum() != baseInterface.dataCheckSum()) {
                    createNewBloomFilter();
                    bloomFilterDataInterface.write(getName(), bloomFilter);
                }
                modifyBloomFilterLock.unlock();
            }
        }
    }

    @Override
    public void dropAllData() {
        modifyBloomFilterLock.lock();
        try {
            baseInterface.dropAllData();
            bloomFilterDataInterface.remove(getName());
            bloomFilter = null;
        } finally {
            modifyBloomFilterLock.unlock();
        }
    }

    @Override
    public boolean mightContain(long key) {
        checkInitialization();
        if (bloomFilter == null) {
            //we are still creating the bloom filter
            return baseInterface.mightContain(key);
        } else {
            return bloomFilter.mightContain(key);
        }
    }

    @Override
    public void valuesChanged(long[] keys) {
        super.valuesChanged(keys);
        modifyBloomFilterLock.lock();
        if (bloomFilter != null) {
            for (Long key : keys) {
                bloomFilter.put(key);
            }
            if (bloomFilter.expectedFpp() > MAX_FPP) {
                bloomFilter = null;
            }
        }
        modifyBloomFilterLock.unlock();
    }
}
