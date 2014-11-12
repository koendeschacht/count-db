package be.bagofwords.db.bloomfilter;

import be.bagofwords.db.DataInterface;
import be.bagofwords.db.LayeredDataInterface;
import be.bagofwords.iterator.CloseableIterator;
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

    private long actualWriteCount;
    private long writeCountOfSavedFilter;

    public BloomFilterDataInterface(DataInterface<T> baseInterface, DataInterface<LongBloomFilterWithCheckSum> bloomFilterDataInterface) {
        super(baseInterface);
        this.bloomFilterDataInterface = bloomFilterDataInterface;
        this.modifyBloomFilterLock = new ReentrantLock();
        this.bloomFilter = bloomFilterDataInterface.read(getName());
        if (this.bloomFilter != null) {
            actualWriteCount = writeCountOfSavedFilter = this.bloomFilter.getDataCheckSum();
        } else {
            writeCountOfSavedFilter = -Long.MAX_VALUE;
            actualWriteCount = writeCountOfSavedFilter + 1;
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
        return bloomFilter != null && actualWriteCount == bloomFilter.getDataCheckSum();
    }

    @Override
    public void write(long key, T value) {
        tryToUpdateFilter(key);
        baseInterface.write(key, value);
    }

    private void tryToUpdateFilter(long key) {
        LongBloomFilterWithCheckSum currFilter = bloomFilter;
        if (currFilter != null) {
            //try to keep filter up-to-date
            currFilter.put(key);
            currFilter.increaseDataCheckSum();
        }
        if (currFilter != null && currFilter.expectedFpp() > MAX_FPP) {
            modifyBloomFilterLock.lock();
            if (bloomFilter != null && bloomFilter.expectedFpp() > MAX_FPP) {
                bloomFilter = null;
            }
            modifyBloomFilterLock.unlock();
        }
        actualWriteCount++;
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
                tryToUpdateFilter(next.getKey());
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
            actualWriteCount = 0;
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
        long numOfValuesForBloomFilter = baseInterface.apprSize();
        bloomFilter = new LongBloomFilterWithCheckSum(numOfValuesForBloomFilter, INITIAL_FPP);
        bloomFilter.setDataCheckSum(actualWriteCount);
        baseInterface.flush();
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
//        if (numOfKeys > 0) {
//            UI.write("Created bloomfilter " + getName() + " in " + taken + " ms for " + numOfKeys + " keys, size is " + bloomFilter.getBits().size() / (8 * 1024) + " kbytes.");
//        }
    }

    private void createNewBloomFilter() {
        modifyBloomFilterLock.lock();
        createNewBloomFilterNonSynchronized();
        modifyBloomFilterLock.unlock();
    }

    @Override
    public synchronized void flush() {
        doActionIfNotClosed(() -> {
            baseInterface.flush();
            writeBloomFilterToDiskIfNecessary();
        });
    }

    private void writeBloomFilterToDiskIfNecessary() {
        modifyBloomFilterLock.lock();
        boolean needsToBeWritten;
        if (bloomFilter == null) {
            needsToBeWritten = writeCountOfSavedFilter != -Long.MAX_VALUE;
        } else {
            needsToBeWritten = writeCountOfSavedFilter != bloomFilter.getDataCheckSum() && bloomFilter.getDataCheckSum() == actualWriteCount;
        }
        if (needsToBeWritten) {
            bloomFilterDataInterface.write(getName(), bloomFilter);
            bloomFilterDataInterface.flush();
            if (bloomFilter == null) {
                writeCountOfSavedFilter = -Long.MAX_VALUE;
            } else {
                writeCountOfSavedFilter = bloomFilter.getDataCheckSum();
            }
        }
        modifyBloomFilterLock.unlock();
    }

    @Override
    protected void doCloseImpl() {
        writeBloomFilterToDiskIfNecessary();
        bloomFilter = null;
    }
}
