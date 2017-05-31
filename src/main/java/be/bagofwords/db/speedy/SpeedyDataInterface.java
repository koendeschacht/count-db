package be.bagofwords.db.speedy;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.MappedLists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.util.*;

import static be.bagofwords.util.Utils.noException;
import static com.sun.xml.internal.fastinfoset.algorithm.IntegerEncodingAlgorithm.INT_SIZE;
import static com.sun.xml.internal.fastinfoset.algorithm.IntegerEncodingAlgorithm.LONG_SIZE;

/**
 * Created by koen on 29/05/17.
 */
public class SpeedyDataInterface<T> extends CoreDataInterface<T> {

    private static final int BATCH_SIZE_PRIMITIVE_VALUES = 100000;
    private static final int BATCH_SIZE_NON_PRIMITIVE_VALUES = 100;
    private final long NULL_VALUE = Long.MIN_VALUE;
    private final long MAX_WRITE_SIZE = 50 * 1024 * 1024;
    private final File directory;
    private final int lengthOfData;
    private SpeedyFile fileNode;
    private double MAX_LOAD_FACTOR = 0.5;
    private int INITIAL_GAP_RATIO = 10;

    private int writes = 0;
    private int skipsInWrites = 0;
    private int reads = 0;
    private int skipsInReads = 0;
    private int numOfWritesOneByOne = 0;
    private int numOfWritesWithRewrite = 0;

    public SpeedyDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, boolean isTemporary, String rootDirectory) {
        super(name, objectClass, combinator, isTemporary);
        if (objectClass != Long.class) {
            throw new RuntimeException("Only works for longs for now");
        }
        this.lengthOfData = determineLengthOfData(objectClass);
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
            String[] files = directory.list();
            for (String fileName : files) {
                File file = new File(directory, fileName);
                if (file.exists() && !file.delete()) {
                    throw new RuntimeException("Failed to delete file " + file.getAbsolutePath());
                }
            }
            fileNode = new SpeedyFile(Long.MIN_VALUE, Long.MAX_VALUE);
            addNodesRecursively(fileNode, Long.MAX_VALUE / 8);
            fileNode.doForAllLeafs(leaf -> {
                File file = getFile(leaf);
                noException(() -> {
                    if (!file.exists() && !file.createNewFile()) {
                        throw new RuntimeException("Failed to create file " + file.getAbsolutePath());
                    }
                });
            });
        });
    }

    private void addNodesRecursively(SpeedyFile node, long maxInterval) {
        if ((double) node.lastKey - node.firstKey > maxInterval) {
            long split = node.firstKey + node.lastKey / 2 - node.firstKey / 2;
            node.left = new SpeedyFile(node.firstKey, split);
            addNodesRecursively(node.left, maxInterval);
            node.right = new SpeedyFile(split, node.lastKey);
            addNodesRecursively(node.right, maxInterval);
        }
    }

    @Override
    public T read(long key) {
        key = rehashKey(key);
        SpeedyFile speedyFile = findFile(key, true);
        if (speedyFile.size == 0) {
            return null;
        }
        File file = getFile(speedyFile);
        RandomAccessFile randomAccessFile = null;
        reads++;
        try {
            randomAccessFile = new RandomAccessFile(file, "r");
            // Log.i("Reading " + key + " from position " + position + " in file " + speedyFile);
            randomAccessFile.seek(findFilePosition(speedyFile, key));
            boolean foundEnd = false;
            while (!foundEnd) {
                if (randomAccessFile.getFilePointer() == speedyFile.size) {
                    randomAccessFile.seek(0);
                }
                // Log.i("Reading " + key + " from position " + position + " in file " + speedyFile);
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
                    //Skip value
                    skipsInReads++;
                    randomAccessFile.seek(randomAccessFile.getFilePointer() + LONG_SIZE);
                }
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
        return (int) (Math.abs(key) % (Math.max(1, file.numOfKeys) * INITIAL_GAP_RATIO));
    }

    private SpeedyFile findFile(long key, boolean lockRead) {
        SpeedyFile file = fileNode.getFile(key, lockRead);
        if (file.firstKey > key || file.lastKey < key) {
            throw new RuntimeException("Huh?");
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
        return fileNode.getTotalNumberOfKeys();
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        long batchSize = getBatchSize();
        List<Pair<Long, T>> values = new ArrayList<>();
        while (entries.hasNext()) {
            KeyValue<T> next = entries.next();
            values.add(Pair.of(rehashKey(next.getKey()), next.getValue()));
            if (values.size() == batchSize) {
                writeAllValues(values);
                values.clear();
            }
        }
        if (!values.isEmpty()) {
            writeAllValues(values);
        }
    }

    @Override
    public void write(long key, T value) {
        key = rehashKey(key);
        SpeedyFile speedyFile = findFile(key, false);
        RandomAccessFile randomAccessFile = null;
        try {
            speedyFile = checkForRewrite(key, speedyFile);
            File file = getFile(speedyFile);
            randomAccessFile = new RandomAccessFile(file, "rw");
            writeSingleValue(key, value, speedyFile, randomAccessFile);
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to file " + speedyFile, exp);
        } finally {
            speedyFile.unlockWrite();
            IOUtils.closeQuietly(randomAccessFile);
        }
    }

    private void writeSingleValue(long key, T value, SpeedyFile speedyFile, RandomAccessFile randomAccessFile) throws IOException {
        //TODO: remove this test after fixing all bugs
        if (speedyFile.firstKey > key || speedyFile.lastKey <= key) {
            throw new RuntimeException("This not the right file!");
        }
        randomAccessFile.seek(findFilePosition(speedyFile, key));
        boolean didWriteValue = false;
        writes++;
        while (!didWriteValue) {
            if (randomAccessFile.getFilePointer() == speedyFile.size) {
                randomAccessFile.seek(0);
            }
            long keyAtPos = randomAccessFile.readLong();
            if (keyAtPos == key) {
                if (value == null) {
                    // Log.i("Writing null value for " + key + " at position " + position + " in " + speedyFile);
                    randomAccessFile.writeLong(NULL_VALUE);
                } else {
                    // Log.i("Writing " + key + " at position " + position + " in " + speedyFile);
                    T currentValue = (T) (Object) randomAccessFile.readLong();
                    T newValue = combinator.combine(currentValue, value);
                    randomAccessFile.seek(randomAccessFile.getFilePointer() - LONG_SIZE);
                    randomAccessFile.writeLong((Long) newValue);
                }
                didWriteValue = true;
            } else if (keyAtPos == NULL_VALUE) {
                if (value != null) {
                    // Log.i("Writing " + key + " at position " + position + " in " + speedyFile);
                    randomAccessFile.seek(randomAccessFile.getFilePointer() - LONG_SIZE);
                    randomAccessFile.writeLong(key);
                    randomAccessFile.writeLong((Long) value);
                    speedyFile.actualKeys++;
                }
                didWriteValue = true;
            } else {
                skipsInWrites++;
                randomAccessFile.skipBytes(LONG_SIZE);
            }
        }
    }

    private SpeedyFile checkForRewrite(long key, SpeedyFile speedyFile) throws IOException {
        double loadFactor = computeLoadFactor(speedyFile);
        while (loadFactor >= MAX_LOAD_FACTOR) {
            rewriteFile(speedyFile);
            speedyFile.unlockWrite();
            speedyFile = findFile(key, false);
            loadFactor = computeLoadFactor(speedyFile);
        }
        return speedyFile;
    }

    private void rewriteFile(SpeedyFile speedyFile) throws IOException {
        Log.i("Rewriting file " + speedyFile);
        List<Pair<Long, T>> values = readAllValues(speedyFile);
        values.sort(Comparator.comparingLong(Pair::getLeft));
        rewriteFile(speedyFile, values);
        // Log.i("Finished rewriting " + speedyFile);
    }

    private void rewriteFile(SpeedyFile speedyFile, List<Pair<Long, T>> values) {
        long newSize = Math.round((speedyFile.numOfKeys + 1) * LONG_SIZE * LONG_SIZE * INITIAL_GAP_RATIO);
        if (newSize > MAX_WRITE_SIZE) {
            //Split in two
            // Log.i("Splitting file " + speedyFile);
            int splitInd = values.size() / 2;
            List<Pair<Long, T>> leftValues = values.subList(0, splitInd);
            List<Pair<Long, T>> rightValues = values.subList(splitInd, values.size());
            long splitKey = values.get(splitInd).getLeft();

            speedyFile.left = new SpeedyFile(speedyFile.firstKey, splitKey);
            speedyFile.right = new SpeedyFile(splitKey, speedyFile.lastKey);

            speedyFile.left.lockWrite();
            rewriteAllValues(leftValues, speedyFile.left);
            speedyFile.left.unlockWrite();

            speedyFile.right.lockWrite();
            rewriteAllValues(rightValues, speedyFile.right);
            speedyFile.right.unlockWrite();

            speedyFile.numOfKeys = 0;
            speedyFile.actualKeys = 0;
        } else {
            rewriteAllValues(values, speedyFile);
        }
    }

    private void writeAllValues(List<Pair<Long, T>> values) {
        MappedLists<SpeedyFile, Pair<Long, T>> mappedValues = mapValuesToFiles(values);
        for (Map.Entry<SpeedyFile, List<Pair<Long, T>>> entry : mappedValues.entrySet()) {
            SpeedyFile file = entry.getKey();
            file.lockWrite();
            try {
                List<Pair<Long, T>> valuesForFile = entry.getValue();
                if (values.size() > file.numOfKeys / 2) {
                    List<Pair<Long, T>> currentValues = readAllValues(file);
                    currentValues.addAll(valuesForFile);
                    Collections.sort(currentValues);
                    rewriteAllValues(currentValues, file);
                    numOfWritesWithRewrite++;
                } else {
                    file = writeAllValuesOneByOneImpl(valuesForFile, file);
                    numOfWritesOneByOne++;
                }
            } catch (IOException exp) {
                throw new RuntimeException("Failed to write to file " + file, exp);
            } finally {
                file.unlockWrite();
            }
        }
    }

    private MappedLists<SpeedyFile, Pair<Long, T>> mapValuesToFiles(List<Pair<Long, T>> values) {
        Collections.sort(values);
        MappedLists<SpeedyFile, Pair<Long, T>> result = new MappedLists<>();
        fileNode.mapValuesToNodes(result, values, 0, values.size());
        return result;
    }

    // private SpeedyFile writeAllValuesOneByOne(List<Pair<Long, T>> values, SpeedyFile currFile) throws IOException {
    //     List<Pair<Long, T>> valuesForCurrentFile = new ArrayList<>();
    //     valuesForCurrentFile.add(values.get(0));
    //     int ind = 1;
    //     while (ind < values.size()) {
    //         Pair<Long, T> curr = values.get(ind);
    //         if (currFile.lastKey <= curr.getKey()) {
    //             // Log.i("Skipping to next file");
    //             currFile = writeAllValuesOneByOneImpl(valuesForCurrentFile, currFile);
    //             valuesForCurrentFile.clear();
    //             currFile.unlockWrite();
    //             currFile = findFile(curr.getKey(), false);
    //         }
    //         valuesForCurrentFile.add(curr);
    //         ind++;
    //     }
    //     if (!valuesForCurrentFile.isEmpty()) {
    //         currFile = writeAllValuesOneByOneImpl(valuesForCurrentFile, currFile);
    //     }
    //     return currFile;
    // }

    private SpeedyFile writeAllValuesOneByOneImpl(List<Pair<Long, T>> values, SpeedyFile currFile) throws IOException {
        // Log.i("About to write " + values.size() + " values to " + currFile);
        RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(currFile), "rw");
        try {
            for (Pair<Long, T> keyValue : values) {
                Long key = keyValue.getKey();
                T value = keyValue.getValue();
                SpeedyFile newFile = checkForRewrite(key, currFile);
                if (!newFile.handlesKey(key)) {
                    newFile.unlockWrite();
                    newFile = findFile(key, false);
                }
                if (newFile != currFile) {
                    randomAccessFile.close();
                    randomAccessFile = new RandomAccessFile(getFile(newFile), "rw");
                    currFile = newFile;
                }
                writeSingleValue(key, value, currFile, randomAccessFile);
            }
        } finally {
            IOUtils.closeQuietly(randomAccessFile);
        }
        // Log.i("Wrote " + values.size() + " values to " + currFile);
        return currFile;
    }

    private void rewriteAllValues(List<Pair<Long, T>> values, SpeedyFile speedyFile) {
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
                    if (value == null) {
                        dos.writeLong(NULL_VALUE);
                    } else {
                        dos.writeLong((Long) value);
                    }
                }
            }
            dos.close();
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to " + file.getAbsolutePath(), exp);
        }
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
        fileNode.doForAllLeafs((file) -> {
            File fsFile = getFile(file);
            if (fsFile.exists() && !fsFile.delete()) {
                throw new RuntimeException("Failed to delete file " + fsFile.getAbsolutePath());
            }
        });
        initializeEmptyFilesList();
    }

    @Override
    protected void flushImpl() {
        //Always flushed
    }

    @Override
    protected void doClose() {
        //Do something?
        Log.i("Number of files " + fileNode.getNumberOfLeafs());
        fileNode.doForAllLeafs(leaf -> Log.i("\tfile " + leaf.firstKey + " " + leaf.lastKey + " " + leaf.numOfKeys + " " + leaf.actualKeys + " " + computeLoadFactor(leaf)));
        Log.i("Reads " + reads);
        Log.i("Skips per read " + skipsInReads / (double) reads);
        Log.i("Writes " + writes);
        Log.i("Skips per write " + skipsInWrites / (double) writes);
        Log.i("Writes one by one " + numOfWritesOneByOne);
        Log.i("Rewrites " + numOfWritesWithRewrite);
    }

    private long getBatchSize() {
        return lengthOfData == -1 ? BATCH_SIZE_NON_PRIMITIVE_VALUES : BATCH_SIZE_PRIMITIVE_VALUES;
    }

}
