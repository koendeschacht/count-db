package be.bagofwords.db.speedy;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.util.KeyValue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

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
    private final Object accessFileLock = new Object();

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
                if (firstKey == Long.MIN_VALUE) {
                    firstKey = Long.MIN_VALUE + 1l;
                }
                SpeedyFile speedyFile = new SpeedyFile(firstKey, -1, -1, 0, 0);
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
        SpeedyFile speedyFile = findFile(key, true);
        File file = getFile(speedyFile);
        long position = findPosition(speedyFile, key);
        Log.i("Reading key " + key + " from position " + position);
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");
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
                    randomAccessFile.skipBytes(LONG_SIZE);
                }
                position += LONG_SIZE * 2;
                Log.i("Looking for key " + key + " from position " + position);
            }
            return null;
        } catch (IOException exp) {
            throw new RuntimeException("Error in file " + file.getAbsolutePath(), exp);
        } finally {
            speedyFile.unlockRead();
        }
    }

    private File getFile(SpeedyFile file) {
        return new File(directory, Long.toString(file.minKey));
    }

    private long findPosition(SpeedyFile file, long key) {
        if (file.firstKey == -1) {
            return 0;
        }
        long ind;
        if (file.firstKey == file.lastKey) {
            if (file.firstKey <= key) {
                ind = 0;
            } else {
                ind = 1;
            }
        } else {
            ind = (key - file.firstKey) * file.numOfKeys / (file.lastKey - file.firstKey);
        }
        ind = Math.min(ind, file.numOfKeys - 1);
        return ind * LONG_SIZE * 2 * 11;
    }

    private SpeedyFile findFile(long key, boolean lockRead) {
        synchronized (accessFileLock) {
            for (int i = 0; i < files.size(); i++) {
                if (i == files.size() - 1 || files.get(i + 1).firstKey > key) {
                    SpeedyFile file = files.get(i);
                    if (lockRead) {
                        file.lockRead();
                    } else {
                        file.lockWrite();
                    }
                    return file;
                }
            }
            throw new RuntimeException("Missing file?");
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        return new CloseableIterator<KeyValue<T>>() {

            long curr = Long.MIN_VALUE + 1;
            int ind = 0;
            List<Pair<Long, T>> values;

            {
                readValues();
            }

            private void readValues() {
                while (values.isEmpty() && curr != Long.MAX_VALUE) {
                    SpeedyFile file = findFile(curr, true);
                    try {
                        values = readAllValues(file);
                        ind = 0;
                        curr = file.lastKey + 1;
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to read file " + file, e);
                    } finally {
                        file.unlockRead();
                    }
                }
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
                return new KeyValue<>(curr.getLeft(), curr.getRight());
            }
        };
    }

    @Override
    public void optimizeForReading() {
        //TODO
    }

    @Override
    public long apprSize() {
        return 0;
    }

    @Override
    public void write(long key, T value) {
        if (key == NULL_VALUE) {
            throw new RuntimeException("Sorry, the key " + key + " can not be used since it is reserved");
        }
        SpeedyFile speedyFile = findFile(key, false);
        File file = getFile(speedyFile);
        Log.i("Writing key " + key);
        try {
            double loadFactor = computeLoadFactor(speedyFile);
            Log.i("Load factor is " + loadFactor);
            if (loadFactor > MAX_LOAD_FACTOR) {
                rewriteFile(speedyFile);
                speedyFile.unlockWrite();
                speedyFile = findFile(key, false);
                file = getFile(speedyFile);
            }
            long position = findPosition(speedyFile, key);
            RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rw");
            Log.i("Seeking to position " + position);
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
                        Log.i("Setting key " + key + " to null at position " + position);
                        randomAccessFile.writeLong(NULL_VALUE);
                    } else {
                        T currentValue = (T) (Object) randomAccessFile.readLong();
                        T newValue = combinator.combine(currentValue, value);
                        Log.i("Writing key " + key + " at position " + position);
                        randomAccessFile.seek(position + LONG_SIZE);
                        randomAccessFile.writeLong((Long) newValue);
                    }
                    didWriteValue = true;
                } else if (keyAtPos == NULL_VALUE) {
                    if (value != null) {
                        Log.i("Writing key " + key + " at position " + position);
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
                Log.i("Position is now " + position);
            }
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to file " + file.getAbsolutePath(), exp);
        } finally {
            speedyFile.unlockWrite();
        }
    }

    private void rewriteFile(SpeedyFile speedyFile) throws IOException {
        Log.i("Rewriting file " + speedyFile);
        List<Pair<Long, T>> values = readAllValues(speedyFile);
        values.sort(Comparator.comparingLong(Pair::getLeft));
        long newSize = Math.round((speedyFile.numOfKeys + 1) * LONG_SIZE * LONG_SIZE * INITIAL_GAP_RATIO);
        if (newSize > MAX_WRITE_SIZE) {
            Log.i("Splitting file in two");
            //Split in two
            List<Pair<Long, T>> valuesForNewFile = values.subList(values.size() / 2, values.size());
            long splitKey = valuesForNewFile.get(0).getLeft();
            SpeedyFile newSpeedyFile = new SpeedyFile(splitKey, splitKey, valuesForNewFile.get(valuesForNewFile.size() - 1).getKey(), 0, 0);
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
        Log.i("Writing all values to " + speedyFile);
        File file = getFile(speedyFile);
        int newSize = 0;
        try {
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            for (int i = 0; i < values.size(); i++) {
                Pair<Long, T> curr = values.get(i);
                long key = curr.getLeft();
                T value = curr.getRight();
                Log.i("Rewriting key " + key + " at position " + newSize);
                dos.writeLong(key);
                dos.writeLong((Long) value);
                newSize += LONG_SIZE * 2;
                for (int j = 0; j < INITIAL_GAP_RATIO; j++) {
                    dos.writeLong(NULL_VALUE);
                    dos.writeLong(NULL_VALUE);
                    newSize += LONG_SIZE * 2;
                }
            }
            for (int j = 0; j < INITIAL_GAP_RATIO; j++) {
                dos.writeLong(NULL_VALUE);
                dos.writeLong(NULL_VALUE);
                newSize += LONG_SIZE * 2;
            }
            speedyFile.size = newSize;
            speedyFile.numOfKeys = values.size();
            speedyFile.actualKeys = values.size();
            if (values.size() == 0) {
                speedyFile.firstKey = -1;
                speedyFile.lastKey = -1;
            } else {
                speedyFile.firstKey = values.get(0).getKey();
                speedyFile.lastKey = values.get(values.size() - 1).getKey();
            }
            dos.close();
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to " + file.getAbsolutePath(), exp);
        }
        Log.i("Wrote all values to " + speedyFile);
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
