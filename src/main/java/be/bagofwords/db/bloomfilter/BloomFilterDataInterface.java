package be.bagofwords.db.bloomfilter;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.LayeredDataInterface;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;

import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

public class BloomFilterDataInterface<T extends Object> extends LayeredDataInterface<T> {

    private static final double INITIAL_FPP = 0.001;
    private final static double MAX_FPP = INITIAL_FPP * 20;
    private final DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface;
    private final ReentrantLock modifyBloomFilterLock;
    private LongBloomFilterWithCheckSum bloomFilter;
    private long currentKeyForNewBloomFilterCreation = Long.MAX_VALUE;

    private long currentWriteCount;
    private long writeCountOfSavedFilter;

    public BloomFilterDataInterface(DataInterface<T> baseInterface, DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface) {
        super(baseInterface);
        this.bloomFilterDataInterface = bloomFilterDataInterface;
        this.modifyBloomFilterLock = new ReentrantLock();
        this.bloomFilter = bloomFilterDataInterface.read(getName());
        if (this.bloomFilter != null) {
            writeCountOfSavedFilter = currentWriteCount = this.bloomFilter.getDataCheckSum();
        } else {
            currentWriteCount = -Long.MAX_VALUE;
            writeCountOfSavedFilter = Long.MAX_VALUE;
        }
    }

    @Override
    public void optimizeForReading() {
        baseInterface.optimizeForReading();
        if (!validBloomFilter(this.bloomFilter)) {
            createNewBloomFilter();
        }
    }

    @Override
    public T read(long key) {
        LongBloomFilterWithCheckSum currentBloomFilter = bloomFilter;
        boolean validFilter = validBloomFilter(currentBloomFilter);
        if (!validFilter && modifyBloomFilterLock.tryLock()) {
            createNewBloomFilter();
            currentBloomFilter = bloomFilter;
            modifyBloomFilterLock.unlock();
        }
        if (!validFilter || currentKeyForNewBloomFilterCreation < key) {
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

    private boolean validBloomFilter(LongBloomFilterWithCheckSum bloomFilter) {
        return bloomFilter != null && currentWriteCount == bloomFilter.getDataCheckSum();
    }

    @Override
    public void write(long key, T value) {
        currentWriteCount++;
        baseInterface.write(key, value);
    }

    @Override
    public void write(final Iterator<KeyValue<T>> keyValueIterator) {
        baseInterface.write(new Iterator<KeyValue<T>>() {
            @Override
            public boolean hasNext() {
                return keyValueIterator.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> next = keyValueIterator.next();
                currentWriteCount++;
                return next;
            }

            @Override
            public void remove() {
                keyValueIterator.remove();
            }
        });
    }

    @Override
    public void dropAllData() {
        modifyBloomFilterLock.lock();
        try {
            baseInterface.dropAllData();
            currentWriteCount = 0;
            createNewBloomFilterNonSynchronized();
            writeBloomFilterToDiskIfNecessary();
        } finally {
            modifyBloomFilterLock.unlock();
        }
    }

    @Override
    public boolean mightContain(long key) {
        LongBloomFilterWithCheckSum currentBloomFilter = bloomFilter;
        boolean validFilter = validBloomFilter(currentBloomFilter);
        if (!validFilter && modifyBloomFilterLock.tryLock()) {
            createNewBloomFilter();
            currentBloomFilter = bloomFilter;
            modifyBloomFilterLock.unlock();
        }
        if (!validFilter || currentKeyForNewBloomFilterCreation < key) {
            //we are still creating the bloom filter
            return baseInterface.read(key) != null;
        } else {
            return currentBloomFilter.mightContain(key);
        }
    }

    private void createNewBloomFilterNonSynchronized() {
        currentKeyForNewBloomFilterCreation = Long.MIN_VALUE;
        baseInterface.flush();
        long numOfValuesForBloomFilter = baseInterface.apprSize();
        bloomFilter = new LongBloomFilterWithCheckSum(numOfValuesForBloomFilter, INITIAL_FPP);
        bloomFilter.setDataCheckSum(currentWriteCount);
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
        UI.write("Created bloomfilter " + getName() + " in " + taken + " ms for " + numOfKeys + " keys, size is " + bloomFilter.getBits().size() / (8 * 1024) + " kbytes.");
    }

    private void createNewBloomFilter() {
        modifyBloomFilterLock.lock();
        createNewBloomFilterNonSynchronized();
        modifyBloomFilterLock.unlock();
    }

    @Override
    public void flush() {
        baseInterface.flush();
        writeBloomFilterToDiskIfNecessary();
    }

    @Override
    public void doOccasionalAction() {
        writeBloomFilterToDiskIfNecessary();
        super.doOccasionalAction();
    }

    private void writeBloomFilterToDiskIfNecessary() {
        if (currentWriteCount != writeCountOfSavedFilter) {
            modifyBloomFilterLock.lock();
            bloomFilterDataInterface.write(getName(), bloomFilter);
            bloomFilterDataInterface.flush();
            if (bloomFilter == null) {
                writeCountOfSavedFilter = 0;
            } else {
                writeCountOfSavedFilter = bloomFilter.getDataCheckSum();
            }
            modifyBloomFilterLock.unlock();
        }
    }

    @Override
    protected void doCloseImpl() {
        writeBloomFilterToDiskIfNecessary();
        bloomFilter = null;
    }
}
