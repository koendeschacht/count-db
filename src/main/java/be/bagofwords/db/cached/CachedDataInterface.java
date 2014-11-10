package be.bagofwords.db.cached;

import be.bagofwords.application.memory.MemoryGobbler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.application.memory.MemoryStatus;
import be.bagofwords.cache.Cache;
import be.bagofwords.cache.CachesManager;
import be.bagofwords.db.DataInterface;
import be.bagofwords.db.DataInterfaceFactoryOccasionalActionsThread;
import be.bagofwords.db.LayeredDataInterface;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.SafeThread;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;

public class CachedDataInterface<T extends Object> extends LayeredDataInterface<T> implements MemoryGobbler {

    private Cache<T> readCache;
    private Map<Thread, WriteBuffer<T>> writeBufferMapping;
    private final Semaphore writeBufferMappingLock;
    private final MemoryManager memoryManager;
    private final SafeThread initializeCachesThread;
    private final Set<Long> dirtyKeys;

    public CachedDataInterface(MemoryManager memoryManager, CachesManager cachesManager, DataInterface<T> baseInterface) {
        super(baseInterface);
        this.memoryManager = memoryManager;
        this.memoryManager.registerMemoryGobbler(this);
        this.readCache = cachesManager.createNewCache(getName(), baseInterface.getObjectClass());
        this.dirtyKeys = new HashSet<>();
        this.writeBufferMapping = new HashMap<>();
        this.writeBufferMappingLock = new Semaphore(100);
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
        getWriteBuffer().write(key, value);
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

    private WriteBuffer<T> getWriteBuffer() {
        writeBufferMappingLock.acquireUninterruptibly(1);
        WriteBuffer<T> writeBuffer = writeBufferMapping.get(Thread.currentThread());
        if (writeBuffer == null) {
            writeBufferMappingLock.release(1);
            writeBufferMappingLock.acquireUninterruptibly(100);
            writeBuffer = new WriteBuffer<>(this);
            writeBufferMapping.put(Thread.currentThread(), writeBuffer);
            writeBufferMappingLock.release(100);
        } else {
            writeBufferMappingLock.release(1);
        }
        return writeBuffer;
    }


    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        baseInterface.write(entries);
        baseInterface.flush();
        readCache.clear();
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
            writeBufferMapping.clear();
            writeBufferMapping = null;
            baseInterface.close();
        }
    }

    public void flush() {
        flushWriteCache(true);
        baseInterface.flush();
        synchronized (dirtyKeys) {
            for (Long key : dirtyKeys) {
                readCache.remove(key);
            }
            dirtyKeys.clear();
        }
        //cleanup:
        removeUnusedWriteBuffers();
    }

    private void removeUnusedWriteBuffers() {
        writeBufferMappingLock.acquireUninterruptibly(100);
        Iterator<Map.Entry<Thread, WriteBuffer<T>>> iterator = writeBufferMapping.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Thread, WriteBuffer<T>> next = iterator.next();
            if (System.currentTimeMillis() - next.getValue().getTimeOfLastUsage() > 20 * 1000) {
                //not used in last 20s, remove
                iterator.remove();
                next.getValue().flush();  //make sure all lingering data was flushed
            }
        }
        writeBufferMappingLock.release(100);
    }

    private void flushWriteCache(final boolean force) {
        writeBufferMappingLock.acquireUninterruptibly(1);
        Collection<WriteBuffer<T>> buffers = writeBufferMapping.values();
        final CountDownLatch latch = new CountDownLatch(buffers.size());
        for (final WriteBuffer<T> buffer : buffers) {
            new Thread() {
                public void run() {
                    buffer.checkFlush(force ? 0 : DataInterfaceFactoryOccasionalActionsThread.TIME_BETWEEN_FLUSHES);
                    latch.countDown();
                }
            }.start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        writeBufferMappingLock.release(1);
    }

    @Override
    public void dropAllData() {
        stopInitializeCachesThread();
        flushWriteCache(true);
        writeBufferMapping.clear();
        readCache.clear();
        baseInterface.dropAllData();
    }

    public long apprSize() {
        return writeBufferMapping.size() + baseInterface.apprSize();
    }

    private void stopInitializeCachesThread() {
        if (!initializeCachesThread.isFinished()) {
            initializeCachesThread.terminate();
            initializeCachesThread.waitForFinish();
        }
    }

    public void writeValuesFromFlush(List<KeyValue<T>> allValues) {
        baseInterface.write(allValues.iterator());
        synchronized (dirtyKeys) {
            for (KeyValue<T> value : allValues) {
                dirtyKeys.add(value.getKey());
            }
        }
    }

    @Override
    public void freeMemory() {
        //note that the read caches are managed by the CachesManager, this class only manages the write buffer
        flushWriteCache(true);
    }

    @Override
    public String getMemoryUsage() {
        return "write buffers size=" + getSizeOfAllWriteBuffers();
    }

    private long getSizeOfAllWriteBuffers() {
        writeBufferMappingLock.acquireUninterruptibly(1);
        long result = 0;
        for (WriteBuffer<T> writeBuffer : writeBufferMapping.values()) {
            result += writeBuffer.size();
        }
        writeBufferMappingLock.release(1);
        return result;
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


