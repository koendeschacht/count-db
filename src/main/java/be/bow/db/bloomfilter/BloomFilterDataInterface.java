package be.bow.db.bloomfilter;

import be.bow.db.DataInterface;
import be.bow.db.LayeredDataInterface;
import be.bow.iterator.CloseableIterator;
import be.bow.ui.UI;

import java.util.concurrent.locks.ReentrantLock;

public class BloomFilterDataInterface<T extends Object> extends LayeredDataInterface<T> {

    private static final boolean DEBUG = false;

    /**
     * TODO: optimize these variables
     */
    private final static double INITIAL_FPP = 0.001;
    private final static double MAX_FPP = 0.05;
    private final DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface;
    private LongBloomFilterWithCheckSum bloomFilter;
    private ReentrantLock modifyBloomFilterLock;
    private int truePositive;
    private int falsePositive;

    public BloomFilterDataInterface(DataInterface<T> baseInterface, DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface) {
        super(baseInterface);
        this.bloomFilterDataInterface = bloomFilterDataInterface;
        this.modifyBloomFilterLock = new ReentrantLock();
    }

    private LongBloomFilterWithCheckSum createNewBloomFilter() {
        baseInterface.flush(); //Make sure all values are written to disk
        long numOfValuesForBloomFilter = Math.max(1000, baseInterface.apprSize());
        UI.write("Creating bloomfilter " + getName());
        LongBloomFilterWithCheckSum result = new LongBloomFilterWithCheckSum(numOfValuesForBloomFilter, INITIAL_FPP);
        result.setDataCheckSum(baseInterface.dataCheckSum());
        long start = System.currentTimeMillis();
        int numOfKeys = 0;
        CloseableIterator<Long> it = baseInterface.keyIterator();
        while (it.hasNext()) {
            long key = it.next();
            result.put(key);
            numOfKeys++;
        }
        it.close();
        long taken = (System.currentTimeMillis() - start);
        UI.write("Created bloomfilter " + getName() + " in " + taken + " ms for " + numOfKeys + " keys.");
        return result;
    }

    @Override
    public T readInt(long key) {
        checkInitialization();
        if (bloomFilter == null) {
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
                T value = baseInterface.read(key);
                if (DEBUG) {
                    if (value != null) {
                        truePositive++;
                    } else {
                        falsePositive++;
                    }
                }
                return value;
            } else {
                //TODO remove DEBUG
//            T value = baseInterface.read(key);
//            if (value != null) {
//                throw new RuntimeException("Value for key " + key + " was found!");
//            }
                //END DEBUG
                return null;
            }
        }
    }

    private void checkInitialization() {
        if (bloomFilter == null) {
            if (modifyBloomFilterLock.tryLock()) {
                bloomFilter = bloomFilterDataInterface.read(getName());
                if (bloomFilter == null || bloomFilter.getDataCheckSum() != baseInterface.dataCheckSum()) {
                    bloomFilter = createNewBloomFilter();
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

    public int getTruePositive() {
        return truePositive;
    }

    public int getFalsePositive() {
        return falsePositive;
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
