package be.bagofwords.db.speedy;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

import static be.bagofwords.db.speedy.LockMethod.LOCK_READ;
import static be.bagofwords.db.speedy.LockMethod.LOCK_WRITE;
import static be.bagofwords.db.speedy.LockMethod.NO_LOCK;
import static be.bagofwords.util.Utils.noException;

/**
 * Created by koen on 29/05/17.
 */
public class SpeedyDataInterface<T> extends CoreDataInterface<T> {

    private static final int BATCH_SIZE_PRIMITIVE_VALUES = 100000;
    private static final int BATCH_SIZE_NON_PRIMITIVE_VALUES = 100;
    private static final int LONG_SIZE = 8;
    private static final int INT_SIZE = 4;
    private final long NULL_VALUE = Long.MIN_VALUE;
    private final long MAX_WRITE_SIZE = 10 * 1024 * 1024;
    private final File directory;
    private final int lengthOfData;
    private SpeedyFile fileNode;
    private double MAX_LOAD_FACTOR = 0.6;
    private int INITIAL_GAP_RATIO = 3;

    private int writes = 0;
    private int skipsInWrites = 0;
    private int reads = 0;
    private int skipsInReads = 0;
    private int numOfWritesOneByOne = 0;
    private int numOfWritesWithRewrite = 0;
    private byte[] nullBuffer;

    public SpeedyDataInterface(String name, Class<T> objectClass, Combinator<T> combinator, ObjectSerializer<T> objectSerializer, boolean isTemporary, String rootDirectory) {
        super(name, objectClass, combinator, objectSerializer, isTemporary);
        if (objectClass != Long.class) {
            throw new RuntimeException("Only works for longs for now");
        }
        this.lengthOfData = determineLengthOfData(objectClass);
        this.directory = new File(rootDirectory, name);
        this.startFromEmptyDirectory();
        createNullBuffer();
    }

    private void createNullBuffer() {
        nullBuffer = new byte[1024 * LONG_SIZE];
        int ind = 0;
        while (ind < nullBuffer.length) {
            SerializationUtils.longToBytes(NULL_VALUE, nullBuffer, ind);
            ind += LONG_SIZE;
        }
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
            fileNode = createSpeedyFile(Long.MIN_VALUE, Long.MAX_VALUE);
            addNodesRecursively(fileNode, Long.MAX_VALUE / 8);
            fileNode.doForAllLeafs(leaf -> {
                File file = leaf.file;
                noException(() -> {
                    if (!file.exists() && !file.createNewFile()) {
                        throw new RuntimeException("Failed to create file " + file.getAbsolutePath());
                    }
                });
            });
        });
    }

    private SpeedyFile createSpeedyFile(long min, long max) {
        return noException(() -> {
            // RandomAccessFile randomAccessFile = new RandomAccessFile(getFile(min), "rw");
            File file = getFile(Long.toString(min));
            FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            return new SpeedyFile(min, max, fileChannel, file);
        });
    }

    private void addNodesRecursively(SpeedyFile node, long maxInterval) {
        if ((double) node.lastKey - node.firstKey > maxInterval) {
            long split = node.firstKey + node.lastKey / 2 - node.firstKey / 2;
            node.setChildNodes(createSpeedyFile(node.firstKey, split), createSpeedyFile(split, node.lastKey));
            addNodesRecursively(node.getLeft(), maxInterval);
            addNodesRecursively(node.getRight(), maxInterval);
        }
    }

    @Override
    public T read(long key) {
        key = hashKey(key);
        SpeedyFile speedyFile = findFile(key, true);
        if (speedyFile.size == 0) {
            return null;
        }
        RandomAccessFile randomAccessFile = null;
        reads++;
        try {
            randomAccessFile = new RandomAccessFile(speedyFile.file, "r");
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
            throw new RuntimeException("Error in file " + speedyFile, exp);
        } finally {
            speedyFile.unlockRead();
            IOUtils.closeQuietly(randomAccessFile);
        }
    }

    private long hashKey(long key) {
        // key = key ^ HASH_ADD;
        if (key == 1) {
            return 1;
        } else {
            return Long.reverse(key);
        }
    }

    private long invertHashkey(long key) {
        long result;
        if (key == 1) {
            result = 1;
        } else {
            result = Long.reverse(key);
        }
        return result;
        // return result ^ HASH_ADD;
    }

    private File getFile(String name) {
        return new File(directory, name);
    }

    private long findFilePosition(SpeedyFile file, long key) {
        long ind = findRelativePosition(file, key);
        return ind * LONG_SIZE * 2;
    }

    private int findRelativePosition(SpeedyFile file, long key) {
        return (int) (Math.abs(key) % (Math.max(1, file.numOfKeys) * INITIAL_GAP_RATIO));
    }

    private SpeedyFile findFile(long key, boolean lockRead) {
        SpeedyFile file = fileNode.getFile(key, lockRead ? LOCK_READ : LOCK_WRITE);
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
                return new KeyValue<>(invertHashkey(curr.getLeft()), curr.getRight());
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
    public void write(CloseableIterator<KeyValue<T>> entries) {
        long batchSize = getBatchSize();
        List<Pair<Long, T>> values = new ArrayList<>();
        while (entries.hasNext()) {
            KeyValue<T> next = entries.next();
            values.add(Pair.of(hashKey(next.getKey()), next.getValue()));
            if (values.size() == batchSize) {
                writeAllValues(values);
                values.clear();
            }
        }
        if (!values.isEmpty()) {
            writeAllValues(values);
        }
        entries.close();
    }

    @Override
    public void write(long origKey, T value) {
        long key = hashKey(origKey);
        SpeedyFile speedyFile = findFile(key, false);
        try {
            if (needsRewrite(speedyFile, 1)) {
                Log.i("Writing " + origKey + " as a rewrite");
                rewriteFile(speedyFile, Collections.singletonList(Pair.of(key, value)));
            } else {
                try (RandomAccessFile randomAccessFile = new RandomAccessFile(speedyFile.file, "rw")) {
                    writeSingleValue(key, value, speedyFile, randomAccessFile);
                }
            }
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to file " + speedyFile, exp);
        } finally {
            speedyFile.unlockWrite();
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

    private boolean needsRewrite(SpeedyFile speedyFile, int numOfExtraValues) {
        return computeLoadFactor(speedyFile, numOfExtraValues) >= MAX_LOAD_FACTOR;
    }

    private void rewriteFile(SpeedyFile speedyFile, List<Pair<Long, T>> extraValues) throws IOException {
        List<Pair<Long, T>> values = readAllValues(speedyFile);
        values.addAll(extraValues);
        Log.i("About to rewrite " + values.size() + " values to " + speedyFile);
        long newSize = Math.round((speedyFile.numOfKeys + 1) * LONG_SIZE * LONG_SIZE * INITIAL_GAP_RATIO);
        if (newSize > MAX_WRITE_SIZE) {
            //Split in two
            Log.i("Splitting file " + speedyFile);
            int splitInd = values.size() / 2;
            List<Pair<Long, T>> leftValues = values.subList(0, splitInd);
            List<Pair<Long, T>> rightValues = values.subList(splitInd, values.size());
            long splitKey = values.get(splitInd).getLeft();

            speedyFile.setChildNodes(createSpeedyFile(speedyFile.firstKey, splitKey), createSpeedyFile(splitKey, speedyFile.lastKey));

            SpeedyFile left = speedyFile.getLeft();
            left.lockWrite();
            generateFile(leftValues, left);
            left.unlockWrite();

            SpeedyFile right = speedyFile.getRight();
            right.lockWrite();
            generateFile(rightValues, right);
            right.unlockWrite();

            speedyFile.numOfKeys = 0;
            speedyFile.actualKeys = 0;
        } else {
            generateFile(values, speedyFile);
        }
    }

    private void writeAllValues(List<Pair<Long, T>> values) {
        MappedLists<SpeedyFile, Pair<Long, T>> mappedValues = mapValuesToFiles(values);
        for (Map.Entry<SpeedyFile, List<Pair<Long, T>>> entry : mappedValues.entrySet()) {
            SpeedyFile parentFile = entry.getKey();
            List<Pair<Long, T>> parentValues = entry.getValue();
            parentFile.lockWrite();
            MappedLists<SpeedyFile, Pair<Long, T>> finalMap = mapValuesToFiles(parentValues);
            try {
                for (Map.Entry<SpeedyFile, List<Pair<Long, T>>> finalEntry : finalMap.entrySet()) {
                    SpeedyFile file = finalEntry.getKey();
                    List<Pair<Long, T>> childValues = entry.getValue();
                    if (file != parentFile) {
                        Log.i("Switched from " + parentFile + " to " + file);
                        file.lockWrite();
                    }
                    if (needsRewrite(file, childValues.size())) {
                        Log.i("writeAllValues using rewrite");
                        rewriteFile(file, childValues);
                        numOfWritesWithRewrite++;
                    } else {
                        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file.file, "rw")) {
                            Log.i("writeAllValues using writeSingleValue");
                            for (Pair<Long, T> value : childValues) {
                                long key = value.getKey();
                                T obj = value.getValue();
                                writeSingleValue(key, obj, file, randomAccessFile);
                            }
                            numOfWritesOneByOne++;
                        }
                    }
                    if (file != parentFile) {
                        file.unlockWrite();
                    }
                }
            } catch (IOException exp) {
                throw new RuntimeException("Failed to write to file " + parentFile, exp);
            } finally {
                parentFile.unlockWrite();
            }
        }
    }

    private MappedLists<SpeedyFile, Pair<Long, T>> mapValuesToFiles(List<Pair<Long, T>> values) {
        MappedLists<SpeedyFile, Pair<Long, T>> result = new MappedLists<>();
        for (Pair<Long, T> value : values) {
            result.get(fileNode.getFile(value.getKey(), NO_LOCK)).add(value);
        }
        return result;
    }

    private void generateFile(List<Pair<Long, T>> values, SpeedyFile speedyFile) {
        try {
            speedyFile.numOfKeys = values.size();
            speedyFile.actualKeys = values.size();
            Pair[] positionedValues = new Pair[(speedyFile.numOfKeys + 1) * INITIAL_GAP_RATIO];
            for (int i = 0; i < values.size(); i++) {
                Pair<Long, T> curr = values.get(i);
                Long key = curr.getKey();
                T value = curr.getValue();
                //TODO: remove this test after fixing all bugs
                if (speedyFile.firstKey > key || speedyFile.lastKey <= key) {
                    throw new RuntimeException("This not the right file!");
                }
                int ind = findRelativePosition(speedyFile, key);
                ind %= positionedValues.length;
                boolean wroteValue = false;
                while (!wroteValue) {
                    Pair<Long, T> existing = positionedValues[ind];
                    if (existing == null) {
                        positionedValues[ind] = curr;
                        wroteValue = true;
                    } else if (existing.getKey().equals(key)) {
                        if (value == null) {
                            positionedValues[ind] = null;
                        } else {
                            positionedValues[ind] = Pair.of(key, getCombinator().combine(existing.getValue(), value));
                        }
                        wroteValue = true;
                    } else {
                        ind = (ind + 1) % positionedValues.length;
                    }
                }
            }
            speedyFile.size = positionedValues.length * LONG_SIZE * 2;
            byte[] buffer = new byte[(int) speedyFile.size];
            fillWithNulls(buffer);
            for (int i = 0; i < positionedValues.length; i++) {
                Pair<Long, T> curr = positionedValues[i];
                if (curr != null) {
                    long key = curr.getLeft();
                    T value = curr.getRight();
                    int position = i * LONG_SIZE * 2;
                    SerializationUtils.longToBytes(key, buffer, position);
                    if (value == null) {
                        SerializationUtils.longToBytes(NULL_VALUE, buffer, position + LONG_SIZE);
                    } else {
                        SerializationUtils.longToBytes((Long) value, buffer, position + LONG_SIZE);
                    }
                }
            }
            // RandomAccessFile file = speedyFile.writeFile;
            // if (file == null) {
            //     throw new RuntimeException("File for " + speedyFile + " was already closed");
            // }
            // file.seek(0);
            // file.write(buffer);
            FileChannel fileChannel = FileChannel.open(speedyFile.file.toPath(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
            fileChannel.write(byteBuffer);
        } catch (IOException exp) {
            throw new RuntimeException("Failed to write to " + speedyFile, exp);
        }
    }

    private void fillWithNulls(byte[] buffer) {
        int i = 0;
        while (i < buffer.length) {
            System.arraycopy(nullBuffer, 0, buffer, i, Math.min(buffer.length - i, nullBuffer.length));
            i += nullBuffer.length;
        }
    }

    private List<Pair<Long, T>> readAllValues(SpeedyFile speedyFile) throws IOException {
        List<Pair<Long, T>> result = new ArrayList<>();
        if (speedyFile.size == 0) {
            return result;
        }
        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(speedyFile.file)));
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

    private double computeLoadFactor(SpeedyFile speedyFile, int extraKeys) {
        if (speedyFile.size == 0) {
            return 1.0;
        } else {
            return (speedyFile.actualKeys + extraKeys) * LONG_SIZE * 2 / (double) speedyFile.size;
        }
    }

    @Override
    public void dropAllData() {
        fileNode.doForAllLeafs((file) -> {
            File fsFile = file.file;
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

        Log.i("Number of files " + fileNode.getNumberOfLeafs());
        fileNode.doForAllLeafs(leaf -> Log.i("\tfile " + leaf.firstKey + " " + leaf.lastKey + " " + leaf.numOfKeys + " " + leaf.actualKeys + " " + computeLoadFactor(leaf, 0)));
        Log.i("Reads " + reads);
        Log.i("Skips per read " + skipsInReads / (double) reads);
        Log.i("Writes " + writes);
        Log.i("Skips per write " + skipsInWrites / (double) writes);
        Log.i("Writes one by one " + numOfWritesOneByOne);
        Log.i("Rewrites " + numOfWritesWithRewrite);
        //
        fileNode.doForAllLeafs(leaf -> IOUtils.closeQuietly(leaf.writeFile));
    }

    private long getBatchSize() {
        return lengthOfData == -1 ? BATCH_SIZE_NON_PRIMITIVE_VALUES : BATCH_SIZE_PRIMITIVE_VALUES;
    }

}
