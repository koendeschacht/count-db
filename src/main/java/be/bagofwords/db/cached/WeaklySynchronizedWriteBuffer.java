package be.bagofwords.db.cached;

import be.bagofwords.cache.DynamicMap;
import be.bagofwords.db.DataInterfaceFactoryOccasionalActionsThread;
import be.bagofwords.util.KeyValue;

import java.util.List;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 27/10/14.
 */
public class WeaklySynchronizedWriteBuffer<T> {

    private final static int MAX_VALUES_TO_CACHE = 200 * 1000;

    private long timeOfLastUsage;
    private long timeOfLastFlush;
    private DynamicMap<T> writeCache;
    private CachedDataInterface<T> dataInterface;

    private final Object lock = new Object();

    public WeaklySynchronizedWriteBuffer(CachedDataInterface<T> dataInterface) {
        this.writeCache = new DynamicMap<>(dataInterface.getObjectClass());
        this.dataInterface = dataInterface;
        this.timeOfLastFlush = System.currentTimeMillis();
        this.timeOfLastUsage = System.currentTimeMillis();
    }

    public void write(Long key, T value) {
        synchronized (lock) {
            checkFlush(DataInterfaceFactoryOccasionalActionsThread.TIME_BETWEEN_FLUSHES / 2);
            nonSynchronizedWrite(key, value);
        }
    }

    private void nonSynchronizedWrite(long key, T value) {
        timeOfLastUsage = System.currentTimeMillis();
        KeyValue<T> cachedValue = writeCache.get(key);
        if (cachedValue == null) {
            //first write of this key
            writeCache.put(key, value);
        } else {
            if (value != null && cachedValue.getValue() != null) {
                T combinedValue = dataInterface.getCombinator().combine(cachedValue.getValue(), value);
                writeCache.put(key, combinedValue);
            } else {
                writeCache.put(key, value);
            }
        }
    }

    public void checkFlush(long time) {
        if (time == 0 || System.currentTimeMillis() - timeOfLastFlush >= time || writeCache.size() > MAX_VALUES_TO_CACHE) {
            flush();
        }
    }

    public void flush() {
        synchronized (lock) {
            List<KeyValue<T>> allValues = writeCache.removeAllValues();
            if (!allValues.isEmpty()) {
                dataInterface.writeValuesFromFlush(allValues);
            }
            timeOfLastFlush = System.currentTimeMillis();
        }
    }

    public long getTimeOfLastUsage() {
        return timeOfLastUsage;
    }
}
