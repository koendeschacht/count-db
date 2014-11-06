package be.bagofwords.db.cached;

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

public class CachedDataInterface<T extends Object> extends LayeredDataInterface<T> {

    private Cache<T> readCache;
    private Map<Thread, WeaklySynchronizedWriteBuffer<T>> writeCache;
    private final Semaphore writeCacheLock;
    private final MemoryManager memoryManager;
    private final SafeThread initializeCachesThread;

    public CachedDataInterface(MemoryManager memoryManager, CachesManager cachesManager, DataInterface<T> baseInterface) {
        super(baseInterface);
        this.memoryManager = memoryManager;
        this.readCache = cachesManager.createNewCache(getName(), baseInterface.getObjectClass());
        this.writeCache = new HashMap<>();
        this.writeCacheLock = new Semaphore(100);
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

    private WeaklySynchronizedWriteBuffer<T> getWriteBuffer() {
        writeCacheLock.acquireUninterruptibly(1);
        WeaklySynchronizedWriteBuffer<T> weaklySynchronizedWriteBuffer = writeCache.get(Thread.currentThread());
        if (weaklySynchronizedWriteBuffer == null) {
            writeCacheLock.release(1);
            writeCacheLock.acquireUninterruptibly(100);
            weaklySynchronizedWriteBuffer = new WeaklySynchronizedWriteBuffer<>(this);
            writeCache.put(Thread.currentThread(), weaklySynchronizedWriteBuffer);
            writeCacheLock.release(100);
        } else {
            writeCacheLock.release(1);
        }
        return weaklySynchronizedWriteBuffer;
    }


    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        writeAndInvalidateReadCacheEntries(entries);
    }

    private void writeAndInvalidateReadCacheEntries(final Iterator<KeyValue<T>> entries) {
        baseInterface.write(new Iterator<KeyValue<T>>() {
            @Override
            public boolean hasNext() {
                return entries.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> next = entries.next();
                readCache.remove(next.getKey());
                return next;
            }

            @Override
            public void remove() {
                entries.remove();
            }
        });
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
            writeCache.clear();
            writeCache = null;
            baseInterface.close();
        }
    }

    public void flush() {
        flushWriteCache(true);
        baseInterface.flush();
    }

    @Override
    public void doOccasionalAction() {
        flushWriteCache(false);
        removeUnusedWriteBuffers();
        super.doOccasionalAction();
    }

    private void removeUnusedWriteBuffers() {
        writeCacheLock.acquireUninterruptibly(100);
        Iterator<Map.Entry<Thread, WeaklySynchronizedWriteBuffer<T>>> iterator = writeCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Thread, WeaklySynchronizedWriteBuffer<T>> next = iterator.next();
            if (System.currentTimeMillis() - next.getValue().getTimeOfLastUsage() > 20 * 1000) {
                //not used in last 20s, remove
                iterator.remove();
                next.getValue().flush();  //make sure all lingering data was flushed
                UI.write("Removed write buffer for " + getName());
            }
        }
        writeCacheLock.release(100);
    }

    private void flushWriteCache(final boolean force) {
        writeCacheLock.acquireUninterruptibly(1);
        Collection<WeaklySynchronizedWriteBuffer<T>> buffers = writeCache.values();
        final CountDownLatch latch = new CountDownLatch(buffers.size());
        for (final WeaklySynchronizedWriteBuffer<T> buffer : buffers) {
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
        writeCacheLock.release(1);
    }

    @Override
    public void dropAllData() {
        stopInitializeCachesThread();
        flushWriteCache(true);
        writeCache.clear();
        readCache.clear();
        baseInterface.dropAllData();
    }

    public long apprSize() {
        return writeCache.size() + baseInterface.apprSize();
    }

    private void stopInitializeCachesThread() {
        if (!initializeCachesThread.isFinished()) {
            initializeCachesThread.terminate();
            initializeCachesThread.waitForFinish();
        }
    }

    public void writeValuesFromFlush(List<KeyValue<T>> allValues) {
        writeAndInvalidateReadCacheEntries(allValues.iterator());

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
                UI.write("Could not add (all) values to cache of " + getName() + " because memory was full");
            } else {
                if (numOfValuesWritten > 0) {
                    UI.write("Added " + numOfValuesWritten + " values to cache of " + getName() + " in " + (System.currentTimeMillis() - start) + " ms");
                }
            }
            iterator.close();
        }
    }
}


