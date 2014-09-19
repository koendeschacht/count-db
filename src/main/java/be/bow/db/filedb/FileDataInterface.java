package be.bow.db.filedb;

import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryGobbler;
import be.bow.db.CoreDataInterface;
import be.bow.db.combinator.Combinator;
import be.bow.iterator.CloseableIterator;
import be.bow.iterator.IterableUtils;
import be.bow.iterator.SimpleIterator;
import be.bow.ui.UI;
import be.bow.util.KeyValue;
import be.bow.util.MappedLists;
import be.bow.util.Pair;
import be.bow.util.SerializationUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;

public class FileDataInterface<T extends Object> extends CoreDataInterface<T> implements MemoryGobbler {

    private static final long MAX_FILE_SIZE_WRITE = 10 * 1024 * 1024;
    private static final long MAX_FILE_SIZE_READ = 1024 * 1024;
    private static final long BITS_TO_DISCARD_FOR_FILE_BUCKETS = 58;
    private static final int BATCH_SIZE = 1000000;
    private static final String CLEAN_FILES_FILE = "CLEAN_FILES";

    /**
     * Special values
     */
    private static final long LONG_NULL = Long.MAX_VALUE;
    private static final int INT_NULL = Integer.MAX_VALUE;
    private static final double DOUBLE_NULL = Double.MAX_VALUE;
    private static final float FLOAT_NULL = Float.MAX_VALUE;

    private final OpenFilesManager openFilesManager;

    private final T nullValue;
    private File directory;
    private FileBucket[] fileBuckets;

    private long timeOfLastWrite;
    private long timeOfLastRead;
    private long timeOfLastWriteOfCleanFilesList;

    public FileDataInterface(OpenFilesManager openFilesManager, Combinator<T> combinator, Class<T> objectClass, String directory, String nameOfSubset) {
        super(nameOfSubset, objectClass, combinator);
        this.openFilesManager = openFilesManager;
        this.directory = new File(directory, nameOfSubset);
        this.nullValue = getNullValueForType(objectClass);
        initializeFileBuckets();
        checkDataDir();
        timeOfLastRead = 0;
        timeOfLastWrite = timeOfLastWriteOfCleanFilesList = System.currentTimeMillis();
    }

    @Override
    public T readInt(long key) {
        FileBucket bucket = getBucket(key);
        bucket.lockRead();
        FileInfo file = bucket.getFile(key);
        try {
            if (fileShouldBeRewrittenForReading(file)) {
                bucket.unlockRead();
                bucket.lockWrite();
                file = bucket.getFile(key);
                rewriteFile(bucket, file, false, MAX_FILE_SIZE_READ);
                bucket.unlockWrite();
                bucket.lockRead();
            }
            T result = readSingleValue(key, file);
            timeOfLastRead = System.currentTimeMillis();
            return result;
        } catch (Exception exp) {
            throw new RuntimeException("Error in file " + toFile(file).getAbsolutePath(), exp);
        } finally {
            bucket.unlockRead();
        }
    }

    @Override
    public void writeInt0(long key, T value) {
        checkNotSpecial(value);
        FileBucket bucket = getBucket(key);
        bucket.lockWrite();
        int fileInd = bucket.getFileInd(key);
        FileInfo file = bucket.getFiles().get(fileInd);
        try {
            DataOutputStream dos = getOutputStream(file, true);
            long extraSize = writeValue(dos, key, value, getObjectClass());
            file.increaseSize(extraSize, false);
            dos.close();
            rewriteFileAfterWriteIfNecessary(bucket, file);
            timeOfLastWrite = System.currentTimeMillis();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            bucket.unlockWrite();
        }
    }

    @Override
    public void writeInt0(Iterator<KeyValue<T>> entries) {
        while (entries.hasNext()) {
            MappedLists<FileBucket, KeyValue<T>> entriesToFileBuckets = new MappedLists<>();
            int numRead = 0;
            while (numRead < BATCH_SIZE && entries.hasNext()) {
                KeyValue<T> curr = entries.next();
                checkNotSpecial(curr.getValue());
                FileBucket fileBucket = getBucket(curr.getKey());
                entriesToFileBuckets.get(fileBucket).add(curr);
                numRead++;
            }
            for (FileBucket bucket : entriesToFileBuckets.keySet()) {
                List<KeyValue<T>> values = entriesToFileBuckets.get(bucket);
                bucket.lockWrite();
                try {
                    MappedLists<FileInfo, KeyValue<T>> entriesToFiles = new MappedLists<>();
                    for (KeyValue<T> value : values) {
                        FileInfo file = bucket.getFile(value.getKey());
                        entriesToFiles.get(file).add(value);
                    }
                    for (FileInfo file : entriesToFiles.keySet()) {
                        List<KeyValue<T>> valuesForFile = entriesToFiles.get(file);
                        DataOutputStream dos = getOutputStream(file, true);
                        for (KeyValue<T> value : valuesForFile) {
                            long extraSize = writeValue(dos, value.getKey(), value.getValue(), getObjectClass());
                            file.increaseSize(extraSize, false);
                        }
                        dos.close();
                        rewriteFileAfterWriteIfNecessary(bucket, file);
                    }
                } catch (IOException exp) {
                    throw new RuntimeException(exp);
                } finally {
                    bucket.unlockWrite();
                }
            }
        }
        timeOfLastWrite = System.currentTimeMillis();
    }

    private void checkNotSpecial(T value) {
        if (nullValue != null && nullValue.equals(value)) {
            throw new RuntimeException("Sorry but value " + value + " is a reserved value to indicate null.");
        }
    }

    private void rewriteFileAfterWriteIfNecessary(FileBucket bucket, FileInfo file) {
        boolean needsRewrite;
        long targetSize;
        if (shouldFilesBeOptimizedForReads()) {
            needsRewrite = fileShouldBeRewrittenForReading(file);
            targetSize = MAX_FILE_SIZE_READ;
        } else {
            needsRewrite = file.getSize() > MAX_FILE_SIZE_WRITE;
            targetSize = MAX_FILE_SIZE_WRITE / 4;
        }
        if (needsRewrite) {
            rewriteFile(bucket, file, true, targetSize);
        }
    }

    private boolean shouldFilesBeOptimizedForReads() {
        return System.currentTimeMillis() - timeOfLastRead < 1000;
    }

    private boolean fileShouldBeRewrittenForReading(FileInfo file) {
        return file.getSize() > MAX_FILE_SIZE_READ || file.getSize() - file.getEndOfCleanSection() > MAX_FILE_SIZE_READ / 100;
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        timeOfLastRead = System.currentTimeMillis();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private FileInfo currentFile;
            private Map<Long, T> valuesInFile;

            @Override
            public KeyValue<T> next() throws Exception {
                while (keyIterator.hasNext()) {
                    long key = keyIterator.next();
                    FileBucket currentBucket = getBucket(key);
                    FileInfo file = currentBucket.getFile(key);
                    if (file != currentFile) {
                        currentFile = file;
                        currentBucket.lockRead();
                        valuesInFile = readMap(currentBucket, file);
                        currentBucket.unlockRead();
                    }
                    T value = valuesInFile.get(key);
                    if (value != null) {
                        return new KeyValue<>(key, value);
                    }
                }

                return null;
            }

        });
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        timeOfLastRead = System.currentTimeMillis();
        final CloseableIterator<Pair<FileInfo, FileBucket>> fileIterator = createFileIterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private Iterator<Pair<Long, T>> valuesInFileIt;

            @Override
            public KeyValue<T> next() throws Exception {
                while ((valuesInFileIt == null || !valuesInFileIt.hasNext()) && fileIterator.hasNext()) {
                    Pair<FileInfo, FileBucket> next = fileIterator.next();
                    FileInfo file = next.getFirst();
                    FileBucket bucket = next.getSecond();
                    bucket.lockRead();
                    List<Pair<Long, T>> sortedEntries = readValuesWithCheck(bucket, file);
                    bucket.unlockRead();
                    valuesInFileIt = sortedEntries.iterator();
                }
                if (valuesInFileIt != null && valuesInFileIt.hasNext()) {
                    Pair<Long, T> next = valuesInFileIt.next();
                    return new KeyValue<>(next.getFirst(), next.getSecond());
                } else {
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                fileIterator.close();
            }
        });
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        timeOfLastRead = System.currentTimeMillis();
        final CloseableIterator<Pair<FileInfo, FileBucket>> fileIterator = createFileIterator();
        return IterableUtils.iterator(new SimpleIterator<Long>() {

            private Iterator<Long> keysInFileIt;

            @Override
            public Long next() throws Exception {
                while ((keysInFileIt == null || !keysInFileIt.hasNext()) && fileIterator.hasNext()) {
                    Pair<FileInfo, FileBucket> next = fileIterator.next();
                    FileInfo file = next.getFirst();
                    FileBucket bucket = next.getSecond();
                    bucket.lockRead();
                    List<Long> sortedKeys = readKeys(bucket, file);
                    bucket.unlockRead();
                    keysInFileIt = sortedKeys.iterator();
                }
                if (keysInFileIt != null && keysInFileIt.hasNext()) {
                    return keysInFileIt.next();
                } else {
                    return null;
                }
            }

            @Override
            public void close() throws Exception {
                fileIterator.close();
            }
        });
    }

    @Override
    public void freeMemory() {
        for (FileBucket bucket : fileBuckets) {
            if (bucket.tryLockRead()) {
                for (FileInfo fileInfo : bucket.getFiles()) {
                    fileInfo.discardFileContents();
                }
                bucket.unlockRead();
            }
        }
    }

    @Override
    public long apprSize() {
        int numOfFilesToSample = 100;
        long numOfObjects = 0;
        long sizeOfSampledFiles = 0;
        int numOfSampledFiles = 0;
        long sizeOfAllFiles = 0;
        try {
            CloseableIterator<Pair<FileInfo, FileBucket>> fileIt = createFileIterator();
            while (fileIt.hasNext()) {
                Pair<FileInfo, FileBucket> curr = fileIt.next();
                FileInfo file = curr.getFirst();
                long fileSize = file.getSize();
                if (numOfSampledFiles < numOfFilesToSample) {
                    FileBucket bucket = curr.getSecond();
                    bucket.lockRead();
                    List<Long> keys = readKeys(bucket, file);
                    bucket.unlockRead();
                    numOfObjects += keys.size();
                    sizeOfSampledFiles += fileSize;
                    if (fileSize == 0 && !keys.isEmpty()) {
                        UI.writeError("Something is wrong with file " + file.getFirstKey());
                    }
                    numOfSampledFiles++;
                }
                sizeOfAllFiles += fileSize;
            }
            fileIt.close();
            if (numOfObjects == 0) {
                return 0;
            } else {
                return sizeOfAllFiles * numOfObjects / sizeOfSampledFiles;
            }
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    @Override
    public void flush() {
        //make sure that all writes have finished by obtaining the lock of all file buckets:
        lockAndUnlockAllBuckets();
    }

    @Override
    public void optimizeForReading() {
        timeOfLastRead = System.currentTimeMillis();
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.lockWrite();
            for (int fileInd = 0; fileInd < fileBucket.getFiles().size(); fileInd++) {
                FileInfo file = fileBucket.getFiles().get(fileInd);
                if (file.isDirty()) {
                    rewriteFile(fileBucket, file, false, MAX_FILE_SIZE_READ);
                }
            }
            fileBucket.unlockWrite();
        }
    }

    private void lockAndUnlockAllBuckets() {
        for (FileBucket bucket : fileBuckets) {
            bucket.lockWrite();
            bucket.unlockWrite();
        }
    }

    @Override
    public void close() {
        writeCleanFilesListIfNecessary();
        lockAndUnlockAllBuckets();
    }

    @Override
    public void dropAllData() {
        try {
            writeLockAllBuckets();
            for (FileBucket bucket : fileBuckets) {
                for (FileInfo file : bucket.getFiles()) {
                    deleteFile(file);
                }
                bucket.getFiles().clear();
            }
            writeCleanFilesListNonSynchronized();
            initializeFiles();
            writeUnlockAllBuckets();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void writeLockAllBuckets() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.lockWrite();
        }
    }

    private void writeUnlockAllBuckets() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.unlockWrite();
        }
    }

    private void readLockAllBuckets() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.lockRead();
        }
    }

    private void readUnlockAllBuckets() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.unlockRead();
        }
    }

    private List<Pair<Long, T>> rewriteFile(FileBucket bucket, FileInfo file, boolean inWritePhase, long maxFileSize) {
        try {
            int fileInd = bucket.getFiles().indexOf(file);
            List<Pair<Long, T>> values = readValues(file);
            int filesMergedWithThisFile = inWritePhase ? 0 : mergeFileIfTooSmall(bucket, fileInd, file, maxFileSize, values);
            DataOutputStream dos = getOutputStream(file, false);
            file.setSize(0);
            List<Pair<Long, Integer>> fileLocations = new ArrayList<>();
            for (Pair<Long, T> entry : values) {
                long key = entry.getFirst();
                T value = entry.getSecond();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream tmpOutputStream = new DataOutputStream(bos);
                writeValue(tmpOutputStream, key, value, getObjectClass());
                byte[] dataToWrite = bos.toByteArray();
                if (file.getSize() > 0 && file.getSize() + dataToWrite.length > maxFileSize) {
                    //Create new file
                    if (filesMergedWithThisFile > 0) {
                        throw new RuntimeException("Something went wrong! Merged file and then created new file?");
                    }
                    dos.close();
                    file.fileIsCleaned(sample(fileLocations, 100));
                    fileLocations = new ArrayList<>();
                    file = new FileInfo(key, 0);
                    bucket.getFiles().add(fileInd + 1, file);
                    fileInd++;
                    dos = getOutputStream(file, false);
                }
                fileLocations.add(new Pair<>(key, file.getSize()));
                dos.write(dataToWrite);
                file.increaseSize(dataToWrite.length, true);
            }
            file.fileIsCleaned(sample(fileLocations, 100));
            dos.close();
            return values;
        } catch (IOException exp) {
            throw new RuntimeException("Unexpected exception while rewrite file " + toFile(file).getAbsolutePath(), exp);
        }
    }

    private int mergeFileIfTooSmall(FileBucket bucket, int currentFileInd, FileInfo file, long maxFileSize, List<Pair<Long, T>> values) {
        int nextFileInd = currentFileInd + 1;
        long combinedSize = file.getSize();
        while (nextFileInd < bucket.getFiles().size() && combinedSize + bucket.getFiles().get(nextFileInd).getSize() < maxFileSize) {
            //Combine the files
            FileInfo nextFile = bucket.getFiles().remove(nextFileInd);
            values.addAll(readValues(nextFile));
            combinedSize += nextFile.getSize();
            deleteFile(nextFile);
            //note that nextFileInd does not need to advance, since we just removed the file at that position
        }
        return nextFileInd - currentFileInd - 1;
    }

    private static <T> long writeValue(DataOutputStream dos, long key, T value, Class<T> objectClass) throws IOException {
        dos.writeLong(key);
        if (objectClass == Long.class) {
            Long longValue = (Long) value;
            if (longValue == null) {
                dos.writeLong(LONG_NULL);
            } else {
                dos.writeLong(longValue);
            }
            return 16;
        } else if (objectClass == Double.class) {
            Double doubleValue = (Double) value;
            if (doubleValue == null) {
                dos.writeDouble(DOUBLE_NULL);
            } else {
                dos.writeDouble(doubleValue);
            }
            return 16;
        } else if (objectClass == Integer.class) {
            Integer intValue = (Integer) value;
            if (intValue == null) {
                dos.writeInt(INT_NULL);
            } else {
                dos.writeInt(intValue);
            }
            return 12;
        } else if (objectClass == Float.class) {
            Float floatValue = (Float) value;
            if (floatValue == null) {
                dos.writeFloat(FLOAT_NULL);
            } else {
                dos.writeFloat(floatValue);
            }
            return 12;
        } else {
            if (value == null) {
                dos.writeInt(0);
                return 8 + 4;
            } else {
                byte[] objAsBytes = SerializationUtils.objectToCompressedBytes(value);
                dos.writeInt(objAsBytes.length);
                dos.write(objAsBytes);
                return 8 + 4 + objAsBytes.length;
            }
        }
    }

    private static <T> T readValue(Class<T> objectClass, DataInputStream dis) throws IOException {
        if (objectClass == Long.class) {
            long readValue = dis.readLong();
            if (readValue == LONG_NULL) {
                return null;
            } else {
                return (T) new Long(readValue);
            }
        } else if (objectClass == Double.class) {
            double readValue = dis.readDouble();
            if (readValue == DOUBLE_NULL) {
                return null;
            } else {
                return (T) new Double(readValue);
            }
        } else if (objectClass == Integer.class) {
            int readValue = dis.readInt();
            if (readValue == INT_NULL) {
                return null;
            } else {
                return (T) new Integer(readValue);
            }
        } else if (objectClass == Float.class) {
            float readValue = dis.readFloat();
            if (readValue == FLOAT_NULL) {
                return null;
            } else {
                return (T) new Float(readValue);
            }
        } else {
            int length = dis.readInt();
            if (length == 0) {
                return null;
            } else {
                byte[] objectAsBytes = new byte[length];
                int bytesRead = dis.read(objectAsBytes);
                if (bytesRead < objectAsBytes.length) {
                    throw new RuntimeException("Could not read complete object, expected " + length + " bytes, received " + bytesRead);
                }
                return SerializationUtils.compressedBytesToObject(objectAsBytes, objectClass);
            }
        }
    }

    private void initializeFileBuckets() {
        fileBuckets = new FileBucket[1 << (64 - BITS_TO_DISCARD_FOR_FILE_BUCKETS)];
        long start = Long.MIN_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        long end = Long.MAX_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        int ind = 0;
        for (long val = start; val <= end; val++) {
            long firstKey = val << BITS_TO_DISCARD_FOR_FILE_BUCKETS;
            long lastKey = ((val + 1) << BITS_TO_DISCARD_FOR_FILE_BUCKETS) - 1;
            if (lastKey < firstKey) {
                //overflow
                lastKey = Long.MAX_VALUE;
            }
            fileBuckets[ind++] = new FileBucket(firstKey, lastKey);
        }
    }

    private void checkDataDir() {
        synchronized (FileDataInterface.class) {
            //Important that this is globally synchronized, otherwise we might try to create the same super directory from 2 different threads
            if (!directory.exists()) {
                boolean success = directory.mkdirs();
                if (!success) {
                    throw new RuntimeException("Failed to create directory " + directory.getAbsolutePath());
                }
            }
        }
        if (directory.isFile()) {
            throw new IllegalArgumentException("File should be directory but is file! " + directory.getAbsolutePath());
        }
        try {
            initializeFiles();
        } catch (IOException exp) {
            throw new RuntimeException(exp);
        }
    }

    private void initializeFiles() throws IOException {
        Map<Long, List<Pair<Long, Integer>>> cleanFileInfo = readCleanFileInfo();
        for (String file : this.directory.list()) {
            if (file.matches("-?[0-9]+")) {
                long key = Long.parseLong(file);
                FileBucket bucket = getBucket(key);
                int size = sizeToInteger(new File(directory, file).length());
                FileInfo fileInfo = new FileInfo(key, size);
                boolean isClean = cleanFileInfo.containsKey(key);
                if (isClean) {
                    fileInfo.fileIsCleaned(cleanFileInfo.get(key));
                }
                bucket.getFiles().add(fileInfo);
            }
        }
        for (FileBucket bucket : fileBuckets) {
            if (bucket.getFiles().isEmpty()) {
                //We need at least one file per bucket..
                FileInfo first = new FileInfo(bucket.getFirstKey(), 0);
                try {
                    boolean success = toFile(first).createNewFile();
                    if (!success) {
                        throw new RuntimeException("Failed to create new file " + first + " at " + toFile(first).getAbsolutePath());
                    } else {
                        bucket.getFiles().add(first);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                Collections.sort(bucket.getFiles());
                if (bucket.getFirstKey() != bucket.getFiles().get(0).getFirstKey()) {
                    throw new RuntimeException("Missing file in " + getName() + " ? Expected file " + new File(directory, Long.toString(bucket.getFirstKey())).getAbsolutePath());
                }
            }

        }
    }

    private Map<Long, List<Pair<Long, Integer>>> readCleanFileInfo() {
        Map<Long, List<Pair<Long, Integer>>> result = new HashMap<>();
        File cleanFilesFile = new File(directory, CLEAN_FILES_FILE);
        if (cleanFilesFile.exists()) {
            try {
                DataInputStream dis = new DataInputStream(new FileInputStream(cleanFilesFile));
                boolean finished = false;
                while (!finished) {
                    try {
                        long startKey = dis.readLong();
                        int numOfFileLocations = dis.readInt();
                        List<Pair<Long, Integer>> fileLocations = new ArrayList<>();
                        for (int i = 0; i < numOfFileLocations; i++) {
                            fileLocations.add(new Pair<>(dis.readLong(), dis.readInt()));
                        }
                        result.put(startKey, fileLocations);
                    } catch (EOFException exp) {
                        finished = true;
                    }
                }
            } catch (IOException exp) {
                UI.writeError("Received exception while reading " + cleanFilesFile.getAbsolutePath(), exp);
            }
        }
        return result;
    }

    synchronized void writeCleanFilesListIfNecessary() {
        //use a local variable to store current value of timeOfLastWrite so we don't have to lock any of the file buckets
        long currentTimeOfLastWrite = timeOfLastWrite;
        if (currentTimeOfLastWrite > timeOfLastWriteOfCleanFilesList) {
            readLockAllBuckets();
            writeCleanFilesListNonSynchronized();
            timeOfLastWriteOfCleanFilesList = currentTimeOfLastWrite;
            readUnlockAllBuckets();
        }
    }

    private void writeCleanFilesListNonSynchronized() {
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(directory, CLEAN_FILES_FILE)));
            for (FileBucket bucket : fileBuckets) {
                for (FileInfo fileInfo : bucket.getFiles()) {
                    if (!fileInfo.isDirty() && fileInfo.getSize() > 0) {
                        dos.writeLong(fileInfo.getFirstKey());
                        dos.writeInt(fileInfo.getFileLocations().size());
                        for (Pair<Long, Integer> location : fileInfo.getFileLocations()) {
                            dos.writeLong(location.getFirst());
                            dos.writeInt(location.getSecond());
                        }
                    }
                }
            }
            dos.close();
        } catch (IOException exp) {
            throw new RuntimeException("Received exception while writing list of clean files", exp);
        }
    }

    private FileBucket getBucket(long key) {
        int ind = (int) ((key >> BITS_TO_DISCARD_FOR_FILE_BUCKETS) + fileBuckets.length / 2);
        return fileBuckets[ind];
    }

    private T readSingleValue(long keyToRead, FileInfo file) throws IOException {
        PositionExposingDataInputStream dis;
        //See whether we can skip a portion of the file?
        int pos = file.getFileLocations() != null ? Collections.binarySearch((List) file.getFileLocations(), keyToRead) : -1;
        int startPos;
        if (pos == -1) {
            //Before first key, value can not be in clean part of file
            startPos = file.getEndOfCleanSection();
        } else {
            if (pos < 0) {
                pos = -(pos + 1);
            }
            if (pos == file.getFileLocations().size() || file.getFileLocations().get(pos).getFirst() > keyToRead) {
                pos--;
            }
            startPos = file.getFileLocations().get(pos).getSecond();
        }
        dis = getInputStream(file);
        dis.skipBytes(startPos);
        byte firstByteOfKeyToRead = (byte) (keyToRead >> 56);
        T value = null;
        //read clean part
        while (dis.getPosition() < file.getEndOfCleanSection()) {
            double currentByte = dis.getCurrentByte();
            if (currentByte == firstByteOfKeyToRead) {
                long key = dis.readLong();
                if (key == keyToRead) {
                    value = readValue(getObjectClass(), dis);
                    break;
                } else if (key > keyToRead) {
                    break;
                } else {
                    skipValue(dis);
                }
            } else if (currentByte > firstByteOfKeyToRead) {
                //key too large
                break;
            } else if (currentByte < firstByteOfKeyToRead) {
                //key too small
                dis.skip(8); //skip key
                skipValue(dis); //skip value
            }
        }
        //read dirty part
        dis.setPosition(file.getEndOfCleanSection());
        while (dis.getPosition() < file.getSize()) {
            double currentByte = dis.getCurrentByte();
            if (currentByte != firstByteOfKeyToRead) {
                dis.skip(8); //skip key
                skipValue(dis); //skip value
            } else {
                long key = dis.readLong();
                if (key == keyToRead) {
                    T newValue = readValue(getObjectClass(), dis);
                    if (value == null || newValue == null) {
                        value = newValue;
                    } else {
                        value = getCombinator().combine(value, newValue);
                    }
                } else {
                    skipValue(dis);
                }
            }
        }
        return value;
    }

    private PositionExposingDataInputStream getInputStream(FileInfo file) throws IOException {
        byte[] fileContents = file.getCachedFileContents();
        if (fileContents == null) {
            openFilesManager.registerOpenFile();
            fileContents = new byte[file.getSize()];
            FileInputStream fis = new FileInputStream(toFile(file));
            int bytesRead = fis.read(fileContents);
            if (bytesRead != file.getSize()) {
                throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.getSize() + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
            }
            IOUtils.closeQuietly(fis);
            openFilesManager.registerClosedFile();
            file.setCachedFileContents(fileContents);
        }
        if (fileContents.length != file.getSize()) {
            throw new RuntimeException("Buffer and file size don't match!");
        }
        return new PositionExposingDataInputStream(fileContents);
    }

    private void skipValue(DataInputStream dis) throws IOException {
        //Skip some bytes
        Class<T> objectClass = getObjectClass();
        if (objectClass == Long.class || objectClass == Double.class) {
            dis.skipBytes(8);
        } else if (objectClass == Integer.class || objectClass == Float.class) {
            dis.skipBytes(4);
        } else {
            int length = dis.readInt();
            dis.skipBytes(length);
        }
    }

    private DataOutputStream getOutputStream(FileInfo fileInfo, boolean append) throws FileNotFoundException {
        fileInfo.discardFileContents();
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toFile(fileInfo), append)));
    }

    private File toFile(FileInfo fileInfo) {
        if (directory == null) {
            throw new RuntimeException("Directory is null, probably the data interface was closed already!");
        }
        return new File(directory, Long.toString(fileInfo.getFirstKey()));
    }

    private Map<Long, T> readMap(FileBucket bucket, FileInfo file) {
        List<Pair<Long, T>> values = readValuesWithCheck(bucket, file);
        Map<Long, T> result = new HashMap<>(values.size());
        for (Pair<Long, T> value : values) {
            result.put(value.getFirst(), value.getSecond());
        }
        return result;
    }

    private List<Pair<Long, T>> readValuesWithCheck(FileBucket bucket, FileInfo file) {
        if (file.isDirty()) {
            bucket.unlockRead();
            bucket.lockWrite();
            List<Pair<Long, T>> result = rewriteFile(bucket, file, false, MAX_FILE_SIZE_READ);
            bucket.unlockWrite();
            bucket.lockRead();
            return result;
        }
        return readValues(file);
    }

    private List<Pair<Long, T>> readValues(FileInfo file) {
        try {
            if (file.getSize() > 0) {
                PositionExposingDataInputStream dis = getInputStream(file);
                int expectedNumberOfLongValues = file.getSize() / 16;
                List<Pair<Long, T>> result = new ArrayList<>(expectedNumberOfLongValues);
                if (!file.isDirty()) {
                    while (dis.getPosition() < file.getEndOfCleanSection()) {
                        long key = dis.readLong();
                        T value = readValue(getObjectClass(), dis);
                        result.add(new Pair<>(key, value));
                    }
                    return result;
                } else {
                    //read values in buckets
                    int numberOfBuckets = Math.max(1, expectedNumberOfLongValues / 1000);
                    List[] buckets = new List[numberOfBuckets];
                    for (int i = 0; i < buckets.length; i++) {
                        buckets[i] = new ArrayList(expectedNumberOfLongValues / numberOfBuckets);
                    }
                    long start = file.getFirstKey();
                    long density = (1l << BITS_TO_DISCARD_FOR_FILE_BUCKETS) / numberOfBuckets;
                    while (dis.hasMoreData()) {
                        long key = dis.readLong();
                        T value = readValue(getObjectClass(), dis);
                        int bucketInd = (int) ((key - start) / density);
                        if (bucketInd == buckets.length) {
                            bucketInd--; //rounding error?
                        }
                        buckets[bucketInd].add(new Pair<>(key, value));
                    }
                    for (int bucketInd = 0; bucketInd < buckets.length; bucketInd++) {
                        List<Pair<Long, T>> currentBucket = buckets[bucketInd];
                        Collections.sort(currentBucket, new Comparator<Pair<Long, T>>() {
                            @Override
                            public int compare(Pair<Long, T> o1, Pair<Long, T> o2) {
                                return Long.compare(o1.getFirst(), o2.getFirst());
                            }
                        });
                        //combine values
                        for (int i = 0; i < currentBucket.size(); i++) {
                            Pair<Long, T> currPair = currentBucket.get(i);
                            long currKey = currPair.getFirst();
                            T currVal = currPair.getSecond();
                            for (int j = i + 1; j < currentBucket.size(); j++) {
                                long nextKey = currentBucket.get(j).getFirst();
                                if (nextKey == currKey) {
                                    T nextVal = currentBucket.get(j).getSecond();
                                    T combinedVal;
                                    if (currVal == null || nextVal == null) {
                                        combinedVal = nextVal;
                                    } else {
                                        //Combine values
                                        combinedVal = getCombinator().combine(currVal, nextVal);
                                    }
                                    currPair.setSecond(combinedVal);
                                    currVal = combinedVal;
                                    i++;
                                } else {
                                    break;
                                }
                            }
                            if (currPair.getSecond() != null) {
                                result.add(currPair);
                            }
                        }
                        buckets[bucketInd] = null; //Free some memory
                    }
                }
                return result;
            } else {
                return Collections.emptyList();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("In interface " + getName() + ", tried to read file " + toFile(file).getAbsolutePath(), e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableIterator<Pair<FileInfo, FileBucket>> createFileIterator() {

        return new CloseableIterator<Pair<FileInfo, FileBucket>>() {

            private Iterator<FileInfo> filesForBucket = null;
            private int bucketInd = -1;

            {
                findNextBucket();
            }

            private void findNextBucket() {
                bucketInd++;
                while (bucketInd < fileBuckets.length && !(filesForBucket = new ArrayList<>(fileBuckets[bucketInd].getFiles()).iterator()).hasNext()) {
                    bucketInd++;
                }
                if (bucketInd >= fileBuckets.length) {
                    //did not find non-empty bucket
                    filesForBucket = null;
                }
            }

            @Override
            protected void closeInt() {
                //OK
            }

            @Override
            public boolean hasNext() {
                return filesForBucket != null;
            }

            @Override
            public Pair<FileInfo, FileBucket> next() {
                int currBucket = bucketInd;
                FileInfo currFile = filesForBucket.next();
                if (!filesForBucket.hasNext()) {
                    findNextBucket();
                }
                return new Pair<>(currFile, fileBuckets[currBucket]);
            }
        };
    }

    private List<Long> readKeys(FileBucket bucket, FileInfo file) throws IOException {
        if (file.isDirty()) {
            List<Pair<Long, T>> values = readValuesWithCheck(bucket, file);
            List<Long> keys = new ArrayList<>();
            for (Pair<Long, T> value : values) {
                keys.add(value.getFirst());
            }
            return keys;
        } else {
            return readKeysFromCleanFile(file);
        }
    }

    private List<Long> readKeysFromCleanFile(FileInfo file) throws IOException {
        List<Long> result = new ArrayList<>();
        if (file.getSize() == 0) {
            return result;
        }
        PositionExposingDataInputStream dis = getInputStream(file);
        while (dis.hasMoreData()) {
            result.add(dis.readLong());
            skipValue(dis);
        }
        return result;
    }

    private void deleteFile(FileInfo file) {
        boolean success = toFile(file).delete();
        if (!success) {
            throw new RuntimeException("Failed to delete file " + toFile(file).getAbsolutePath());
        }
    }

    private int sizeToInteger(long size) {
        if (size > Integer.MAX_VALUE) {
            throw new RuntimeException("Size too large!");
        }
        return (int) size;
    }

    private List<Pair<Long, Integer>> sample(List<Pair<Long, Integer>> fileLocations, int invSampleRate) {
        List<Pair<Long, Integer>> result = new ArrayList<>();
        for (int i = 0; i < fileLocations.size(); i++) {
            if (i % invSampleRate == 0) {
                result.add(fileLocations.get(i));
            }
        }
        return result;
    }

    private static <T> T getNullValueForType(Class<T> objectClass) {
        if (objectClass == Long.class) {
            return (T) new Long(Long.MAX_VALUE);
        } else if (objectClass == Double.class) {
            return (T) new Double(Double.MAX_VALUE);
        } else if (objectClass == Float.class) {
            return (T) new Float(Float.MAX_VALUE);
        } else if (objectClass == Integer.class) {
            return (T) new Integer(Integer.MAX_VALUE);
        } else {
            return null;
        }
    }
}
