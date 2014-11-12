package be.bagofwords.db.cached;

import be.bagofwords.application.memory.MemoryGobbler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.application.memory.MemoryStatus;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.cache.DynamicMap;
import be.bagofwords.cache.ReadCache;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.LayeredDataInterface;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SafeThread;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CachedDataInterface<T extends Object> extends LayeredDataInterface<T> implements MemoryGobbler {

    private static final int NUM_OF_WRITE_BUFFERS = 20;

    private ReadCache<T> readCache;
    private List<DynamicMap<T>> writeBuffers;
    private final MemoryManager memoryManager;
    private final SafeThread initializeCachesThread;

    public CachedDataInterface(MemoryManager memoryManager, CachesManager cachesManager, DataInterface<T> baseInterface) {
        super(baseInterface);
        this.memoryManager = memoryManager;
        this.memoryManager.registerMemoryGobbler(this);
        this.readCache = cachesManager.createNewCache(getName(), baseInterface.getObjectClass());
        this.writeBuffers = new ArrayList<>();
        for (int i = 0; i < NUM_OF_WRITE_BUFFERS; i++) {
            this.writeBuffers.add(new DynamicMap<>(getObjectClass()));
        }
        this.initializeCachesThread = new InitializeCachesThread(baseInterface);
        this.initializeCachesThread.start();
    }

    @Override
    public T read(long key) {
        KeyValue<T> cachedValue = readCache.get(key);
        if (cachedValue == null) {
            //never read, read from direct
            T value = baseInterface.read(key);
            readCache.put(key, value);
            return value;
        } else {
            return cachedValue.getValue();
        }
    }

    @Override
    public boolean mightContain(long key) {
        KeyValue<T> cachedValue = readCache.get(key);
        if (cachedValue != null) {
            if (cachedValue.getValue() == null) {
                return false;
            } else {
                return true;
            }
        } else {
            return baseInterface.mightContain(key);
        }
    }

    @Override
    public void write(long key, T value) {
        memoryManager.waitForSufficientMemory();
        int writeBufferInd = (int) (key % NUM_OF_WRITE_BUFFERS);
        if (writeBufferInd < 0) {
            writeBufferInd += NUM_OF_WRITE_BUFFERS;
        }
        DynamicMap<T> writeBuffer = writeBuffers.get(writeBufferInd);
        synchronized (writeBuffer) {
            KeyValue<T> cachedValue = writeBuffer.get(key);
            if (cachedValue == null) {
                //first write of this key
                writeBuffer.put(key, value);
            } else {
                if (value != null && cachedValue.getValue() != null) {
                    T combinedValue = getCombinator().combine(cachedValue.getValue(), value);
                    writeBuffer.put(key, combinedValue);
                } else {
                    writeBuffer.put(key, value);
                }
            }
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> cachedValueIterator() {
        final Iterator<KeyValue<T>> iterator = readCache.iterator();
        return new CloseableIterator<KeyValue<T>>() {
            @Override
            protected void closeInt() {
                //ok
            }

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                return iterator.next();
            }
        };
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        while (entries.hasNext()) {
            KeyValue<T> next = entries.next();
            write(next.getKey(), next.getValue());
        }
    }

    @Override
    public synchronized void doCloseImpl() {
        try {
            stopInitializeCachesThread();
            flush();
        } finally {
            //even if the flush failed, we remove our data structures
            readCache.clear();
            readCache = null;
            writeBuffers = null;
            baseInterface.close();
        }
    }

    public synchronized void flush() {
        doActionIfNotClosed(() -> {
            //flush values in write cache
            List<Long> allKeys = new ArrayList<>();
            writeBuffers.parallelStream().forEach(buffer -> {
                List<KeyValue<T>> valuesInBuffer;
                synchronized (buffer) {
                    valuesInBuffer = buffer.removeAllValues();
                }
                if (!valuesInBuffer.isEmpty()) {
                    baseInterface.write(valuesInBuffer.iterator());
                    synchronized (allKeys) {
                        for (KeyValue<T> value : valuesInBuffer) {
                            allKeys.add(value.getKey());
                        }
                    }
                }
            });

            if (!allKeys.isEmpty()) {
                //flush base interface (changes should now be visible)
                baseInterface.flush();
                //invalidate items in read cache
                for (Long key : allKeys) {
                    readCache.remove(key);
                }
            }
        });
    }

    @Override
    public void dropAllData() {
        stopInitializeCachesThread();
        for (DynamicMap<T> writeBuffer : writeBuffers) {
            synchronized (writeBuffer) {
                writeBuffer.clear();
            }
        }
        readCache.clear();
        baseInterface.dropAllData();
    }

    private void stopInitializeCachesThread() {
        if (!initializeCachesThread.isFinished()) {
            initializeCachesThread.terminate();
            initializeCachesThread.waitForFinish();
        }
    }

    @Override
    public void freeMemory() {
        flush();
    }

    @Override
    public String getMemoryUsage() {
        return "write buffers size=" + apprSize();
    }


    private class InitializeCachesThread extends SafeThread {

        public InitializeCachesThread(DataInterface<T> baseInterface) {
            super("initialize_cache_" + baseInterface.getName(), false);
        }

        @Override
        protected void runInt() throws Exception {
            CloseableIterator<KeyValue<T>> iterator = baseInterface.cachedValueIterator();
            int numOfValuesWritten = 0;
            long start = System.currentTimeMillis();
            while (iterator.hasNext() && memoryManager.getMemoryStatus() == MemoryStatus.FREE && !isTerminateRequested()) {
                KeyValue<T> next = iterator.next();
                readCache.put(next.getKey(), next.getValue());
                numOfValuesWritten++;
            }
            if (iterator.hasNext()) {
                UI.write("Could not add (all) values to cache of " + baseInterface.getName() + " because memory was full");
            } else {
                if (numOfValuesWritten > 0) {
                    UI.write("Added " + numOfValuesWritten + " values to cache of " + baseInterface.getName() + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
            iterator.close();
        }
    }
}


