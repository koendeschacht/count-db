package be.bagofwords.db.speedy;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.util.KeyValue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static be.bagofwords.util.Utils.noException;
import static com.sun.xml.internal.fastinfoset.algorithm.IntegerEncodingAlgorithm.INT_SIZE;
import static com.sun.xml.internal.fastinfoset.algorithm.IntegerEncodingAlgorithm.LONG_SIZE;

/**
 * Created by koen on 29/05/17.
 */
public class SpeedyDataInterface<T> extends CoreDataInterface<T> {

    private final long NULL_VALUE = Long.MIN_VALUE;
    private final long MAX_WRITE_SIZE = 50 * 1024 * 1024;
    private final File directory;
    private final int lengthOfData;
    private List<SpeedyFile> files;
    private double MAX_LOAD_FACTOR = 0.5;
    private int INITIAL_GAP_RATIO = 10;
    private final ReadWriteLock accessFileLock = new ReentrantReadWriteLock();

    public SpeedyDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporary, String rootDirectory) {
        super(name, objectClass, combinator, isTemporary);
        if (objectClass != Long.class) {
            throw new RuntimeException("Only works for longs for now");
        }
        this.lengthOfData = determineLengthOfData(objectClass);
        this.files = new ArrayList<>();
        this.directory = new File(rootDirectory, name);
        this.startFromEmptyDirectory();
    }

    private int determineLengthOfData(Class<T> objectClass) {
        if (objectClass == Long.class || objectClass == Double.class) {
            return LONG_SIZE;
        } else if (objectClass == Integer.class || objectClass == Float.class) {
            return INT_SIZE;
        } else {
            return -1;
        }
    }

    private void startFromEmptyDirectory() {
        noException(() -> {
            if (directory.exists()) {
                FileUtils.deleteDirectory(directory);
            }
        });
        if (!directory.mkdirs()) {
            throw new RuntimeException("Failed to create " + directory.getAbsolutePath());
        }
        initializeEmptyFilesList();
    }

    private void initializeEmptyFilesList() {
        noException(() -> {
            files.clear();
            int BITS_TO_DISCARD_FOR_FILE_BUCKETS = 60;
            long start = Long.MIN_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
            long end = Long.MAX_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
            for (long val = start; val <= end; val++) {
                long firstKey = val << BITS_TO_DISCARD_FOR_FILE_BUCKETS;
                long lastKey = ((val + 1) << BITS_TO_DISCARD_FOR_FILE_BUCKETS);
                if (lastKey < firstKey) {
                    //overflow
                    lastKey = Long.MAX_VALUE;
                }
                if (firstKey == Long.MIN_VALUE) {
                    firstKey = Long.MIN_VALUE + 1l;
                }
                SpeedyFile speedyFile = new SpeedyFile(firstKey, lastKey, 0, 0);
                File file = getFile(speedyFile);
                if (file.exists() && !file.delete()) {
                    throw new RuntimeException("Failed to delete file " + file.getAbsolutePath());
                }
                files.add(speedyFile);
            }
        });
    }

    @Override
    public T read(long key) {
        key = rehashKey(key);
        SpeedyFile speedyFile = findFile(key, true);
        File file = getFile(speedyFile);
        long position = findFilePosition(speedyFile, key);
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            randomAccessFile.seek(position);
            boolean foundEnd = false;
            while (!foundEnd) {
                if (position == speedyFile.size) {
                    position = 0;
                    randomAccessFile.seek(0);
                }
                long keyAtPos = randomAccessFile.readLong();
                if (keyAtPos == key) {
                    long value = randomAccessFile.readLong();
                    if (value == NULL_VALUE) {
                        return null;
                    } else {
                        return (T) (Object) value;
                    }
                } else if (keyAtPos == NULL_VALUE) {
                    foundEnd = true;
                } else {
                    randomAccessFile.seek(position + LONG_SIZE * 2);
                }
                position += LONG_SIZE * 2;
            }
            return null;
        } catch (IOException exp) {
            throw new RuntimeException("Error in file " + file.getAbsolutePath(), exp);
        } finally {
            speedyFile.unlockRead();
            IOUtils.closeQuietly(randomAccessFile);
        }
    }

    private long rehashKey(long key) {
        if (key == 1) {
            return 1;
        } else {
            return Long.reverse(key);
        }
    }

    private File getFile(SpeedyFile file) {
        return new File(directory, Long.toString(file.firstKey));
    }

    private long findFilePosition(SpeedyFile file, long key) {
        long ind = findRelativePosition(file, key);
        return ind * LONG_SIZE * 2;
    }

    private int findRelativePosition(SpeedyFile file, long key) {
        int ind;
        if (file.firstKey == -1) {
            ind = 0;
        } else {
            if (file.firstKey == file.lastKey) {
                if (file.firstKey <= key) {
                    ind = 0;
                } else {
                    ind = 1;
                }
            } else {
                ind = (int) Math.round(((double) (key - file.firstKey)) * file.numOfKeys / (file.lastKey - file.firstKey));
            }
            ind = Math.max(0, Math.min(ind, file.numOfKeys - 1));
        }
        return ind * INITIAL_GAP_RATIO;
    }

    private SpeedyFile findFile(long key, boolean lockRead) {
        if (lockRead) {
            accessFileLock.readLock().lock();
        } else {
            accessFileLock.writeLock().lock();
        }
        int ind = Collections.binarySearch(files, key, (o1, o2) -> {
            SpeedyFile file = (SpeedyFile) o1;
            long key1 = (Long) o2;
            return Long.compare(file.lastKey, key1);
        });
        if (ind < 0) {
            ind = -(ind + 1);
        } else {
            ind++;
        }
        if (ind == files.size()) {
            ind--;
        }
        SpeedyFile file = files.get(ind);
        if (file.firstKey > key || file.lastKey < key) {
            throw new RuntimeException("Huh?");
        }
        if (lockRead) {
            file.lockRead();
        } else {
            file.lockWrite();
        }
        if (lockRead) {
            accessFileLock.readLock().unlock();
        } else {
            accessFileLock.writeLock().unlock();
        }
        return file;
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        return new CloseableIterator<KeyValue<T>>() {

            long curr = Long.MIN_VALUE + 1;
            int ind = 0;
            List<Pair<Long, T>> values = Collections.emptyList();

            {
                readValues();
            }

            private void readValues() {
                do {
                    if (curr != Long.MAX_VALUE) {
                        SpeedyFile file = findFile(curr, true);
                        try {
                            values = readAllValues(file);
                            ind = 0;
                            curr = file.lastKey;
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to read file " + file, e);
                        } finally {
                            file.unlockRead();
                        }
                    } else {
                        values = Collections.emptyList();
                    }
                } while (values.isEmpty() && curr != Long.MAX_VALUE);
            }

            @Override
            protected void closeInt() {
                //Do nothing
            }

            @Override
            public boolean hasNext() {
                return !values.isEmpty();
            }

            @Override
            public KeyValue<T> next() {
                Pair<Long, T> curr = values.get(ind);
                ind++;
                if (ind == values.size()) {
                    readValues();
                }
                return new KeyValue<>(rehashKey(curr.getLeft()), curr.getRight());
            }
        };
    }

    @Override
    public void optimizeForReading() {
        //TODO
    }

    @Override
    public long apprSize() {
        long sum = 0;
        for (SpeedyFile file : files) {
            sum += file.actualKeys;
        }
        return sum;
    }

    @Override
    public void write(long key, T value) {
        key = rehashKey(key);
        SpeedyFile speedyFile = findFile(key, false);
        File file = getFile(speedyFile);
        RandomAccessFile randomAccessFile = null;
        try {
            double loadFactor = computeLoadFactor(speedyFile);
            if (loadFactor >= MAX_LOAD_FACTOR) {
                rewriteFile(speedyFile);
                speedyFile.unlockWrite();
                speedyFile = findFile(key, false);
                file = getFile(speedyFile);
            }
            randomAccessFile = new RandomAccessFile(file, "rw");
            long position = findFilePosition(speedyFile, key);
            if (position < 0) {
                findFilePosition(speedyFile, key);
            }
            randomAccessFile.seek(position);
            boolean didWriteValue = false;
            while (!didWriteValue) {
                if (position == speedyFile.size) {
                    position = 0;
                    randomAccessFile.seek(position);
                }
                long keyAtPos = randomAccessFile.readLong();
                if (keyAtPos == key) {
                    if (value == null) {
                        randomAccessFile.writeLong(NULL_VALUE);
                    } else {
                        T currentValue = (T) (Object) randomAccessFile.readLong();
                        T newValue = combinator.combine(currentValue, value);
                        randomAccessFile.seek(position + LONG_SIZE);
                        randomAccessFile.writeLong((Long) newValue);
                    }
                    didWriteValue = true;
                } else if (keyAtPos == NULL_VALUE) {
                    if (value != null) {
                        randomAccessFile.seek(position);
                        randomAccessFile.writeLong(key);
                        randomAccessFile.writeLong((Long) value);
                        speedyFile.actualKeys++;
                    }
                    didWriteValue = true;
                } else {
                    randomAccessFile.skipBytes(LONG_SIZE);
                    position += LONG_SIZE + LONG_SIZE;
                }
            }
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to file " + file.getAbsolutePath(), exp);
        } finally {
            speedyFile.unlockWrite();
            IOUtils.closeQuietly(randomAccessFile);
        }
    }

    private void rewriteFile(SpeedyFile speedyFile) throws IOException {
        List<Pair<Long, T>> values = readAllValues(speedyFile);
        values.sort(Comparator.comparingLong(Pair::getLeft));
        long newSize = Math.round((speedyFile.numOfKeys + 1) * LONG_SIZE * LONG_SIZE * INITIAL_GAP_RATIO);
        if (newSize > MAX_WRITE_SIZE) {
            //Split in two
            List<Pair<Long, T>> valuesForNewFile = values.subList(values.size() / 2, values.size());
            long splitKey = valuesForNewFile.get(0).getLeft();
            SpeedyFile newSpeedyFile = new SpeedyFile(splitKey, speedyFile.firstKey, 0, 0);
            speedyFile.lastKey = splitKey - 1;
            newSpeedyFile.lockWrite();
            writeAllValues(valuesForNewFile, newSpeedyFile);
            newSpeedyFile.unlockWrite();
            addNewFile(newSpeedyFile);
            values = values.subList(0, values.size() / 2);
        }
        writeAllValues(values, speedyFile);
    }

    private void writeAllValues(List<Pair<Long, T>> values, SpeedyFile speedyFile) {
        File file = getFile(speedyFile);
        try {
            speedyFile.numOfKeys = values.size();
            speedyFile.actualKeys = values.size();
            Pair[] positionedValues = new Pair[(speedyFile.numOfKeys + 1) * INITIAL_GAP_RATIO];
            for (int i = 0; i < values.size(); i++) {
                Pair<Long, T> curr = values.get(i);
                int ind = findRelativePosition(speedyFile, curr.getKey());
                ind %= positionedValues.length;
                while (positionedValues[ind] != null) {
                    ind = (ind + 1) % positionedValues.length;
                }
                positionedValues[ind] = curr;
            }
            speedyFile.size = positionedValues.length * LONG_SIZE * 2;
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            for (int i = 0; i < positionedValues.length; i++) {
                Pair<Long, T> curr = positionedValues[i];
                if (curr == null) {
                    dos.writeLong(NULL_VALUE);
                    dos.writeLong(NULL_VALUE);
                } else {
                    long key = curr.getLeft();
                    T value = curr.getRight();
                    dos.writeLong(key);
                    dos.writeLong((Long) value);
                }
            }
            dos.close();
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to " + file.getAbsolutePath(), exp);
        }
    }

    private void addNewFile(SpeedyFile newSpeedyFile) {
        int ind = Collections.binarySearch(files, newSpeedyFile);
        if (ind >= 0) {
            throw new RuntimeException("Could not add new file " + newSpeedyFile);
        }
        ind = -(ind + 1);
        files.add(ind, newSpeedyFile);
    }

    private List<Pair<Long, T>> readAllValues(SpeedyFile speedyFile) throws IOException {
        File file = getFile(speedyFile);
        List<Pair<Long, T>> result = new ArrayList<>();
        if (speedyFile.size == 0) {
            return result;
        }
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        boolean endReached = false;
        while (!endReached) {
            try {
                long key = dis.readLong();
                if (key == NULL_VALUE) {
                    dis.skipBytes(LONG_SIZE);
                } else {
                    long longValue = dis.readLong();
                    if (longValue != NULL_VALUE) {
                        T value = (T) (Object) longValue;
                        result.add(Pair.of(key, value));
                    }
                }
            } catch (EOFException exp) {
                endReached = true;
            }
        }
        dis.close();
        return result;
    }

    private double computeLoadFactor(SpeedyFile speedyFile) {
        if (speedyFile.size == 0) {
            return 1.0;
        } else {
            return speedyFile.actualKeys * LONG_SIZE * 2 / (double) speedyFile.size;
        }
    }

    @Override
    public void dropAllData() {
        for (SpeedyFile file : files) {
            File fsFile = getFile(file);
            if (fsFile.exists() && !fsFile.delete()) {
                throw new RuntimeException("Failed to delete file " + fsFile.getAbsolutePath());
            }
        }
        initializeEmptyFilesList();
    }

    @Override
    protected void flushImpl() {
        //Always flushed
    }

    @Override
    protected void doClose() {
        //Do something?
    }

}
