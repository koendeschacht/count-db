package be.bagofwords.db.filedb;

import be.bagofwords.application.memory.MemoryGobbler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.application.memory.MemoryStatus;
import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.ui.UI;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.Pair;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.*;

public class FileDataInterface<T extends Object> extends CoreDataInterface<T> implements MemoryGobbler {

    private static final long MAX_FILE_SIZE_WRITE = 10 * 1024 * 1024;
    private static final long MAX_FILE_SIZE_READ = 1024 * 1024;
    private static final long BITS_TO_DISCARD_FOR_FILE_BUCKETS = 58;
    private static final int BATCH_SIZE = 1000000;

    private static final String CLEAN_FILES_FILE = "CLEAN_FILES";
    private static final String LOCK_FILE = "LOCK";

    private static final int LONG_SIZE = 8;
    private static final int INT_SIZE = 4;

    private final String sizeOfCachedFileContentsLock = new String("LOCK");
    private final int sizeOfValues;
    private final long randomId;
    private File directory;
    private FileBucket[] fileBuckets;
    private MemoryManager memoryManager;

    private final long maxSizeOfCachedFileContents = Runtime.getRuntime().maxMemory() / 3;

    private long timeOfLastWrite;
    private long timeOfLastRead;
    private long timeOfLastWriteOfCleanFilesList;
    private long currentSizeOfCachedFileContents;

    public FileDataInterface(MemoryManager memoryManager, Combinator<T> combinator, Class<T> objectClass, String directory, String nameOfSubset) {
        super(nameOfSubset, objectClass, combinator);
        this.directory = new File(directory, nameOfSubset);
        this.sizeOfValues = SerializationUtils.getWidth(objectClass);
        this.randomId = new Random().nextLong();
        this.memoryManager = memoryManager;
        initializeFileBuckets();
        checkDataDir();
        writeLockFile(randomId);
        timeOfLastRead = 0;
        timeOfLastWrite = timeOfLastWriteOfCleanFilesList = System.currentTimeMillis();
        currentSizeOfCachedFileContents = 0;
    }

    private void writeLockFile(long id) {
        File lockFile = new File(directory, LOCK_FILE);
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(lockFile));
            dos.writeLong(id);
            IOUtils.closeQuietly(dos);
        } catch (Exception exp) {
            throw new RuntimeException("Unexpected exception while trying to write lock file to " + lockFile.getAbsolutePath(), exp);
        }
    }

    @Override
    public T read(long key) {
        FileBucket bucket = getBucket(key);
        bucket.lockRead();
        FileInfo file = bucket.getFile(key);
        try {
            while (file.isDirty()) {
                bucket.unlockRead();
                bucket.lockWrite();
                file = bucket.getFile(key); //necessary!
                rewriteFile(bucket, file, false, MAX_FILE_SIZE_READ);
                bucket.unlockWrite();
                bucket.lockRead();
                file = bucket.getFile(key); //necessary!
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
        FileBucket bucket = getBucket(key);
        bucket.lockWrite();
        int fileInd = bucket.getFileInd(key);
        FileInfo file = bucket.getFiles().get(fileInd);
        try {
            DataOutputStream dos = getOutputStream(file, true);
            long extraSize = writeValue(dos, key, value);
            file.increaseSize(extraSize, false);
            dos.close();
            rewriteFileAfterWriteIfNecessary(bucket, file);
            timeOfLastWrite = System.currentTimeMillis();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write value with key " + key + " to file " + toFile(file).getAbsolutePath(), e);
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
                        try {
                            List<KeyValue<T>> valuesForFile = entriesToFiles.get(file);
                            DataOutputStream dos = getOutputStream(file, true);
                            for (KeyValue<T> value : valuesForFile) {
                                long extraSize = writeValue(dos, value.getKey(), value.getValue());
                                file.increaseSize(extraSize, false);
                            }
                            dos.close();
                            rewriteFileAfterWriteIfNecessary(bucket, file);
                        } catch (Exception exp) {
                            throw new RuntimeException("Failed to write multiple values to file " + toFile(file).getAbsolutePath(), exp);
                        }
                    }
                } finally {
                    bucket.unlockWrite();
                }
            }
        }
        timeOfLastWrite = System.currentTimeMillis();
    }

    private void rewriteFileAfterWriteIfNecessary(FileBucket bucket, FileInfo file) {
        boolean needsRewrite;
        long targetSize;
        if (shouldFilesBeOptimizedForReads()) {
            needsRewrite = file.isDirty();
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
        final FileIterator fileIterator = createFileIterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private Iterator<Pair<Long, T>> valuesInFileIt;

            @Override
            public KeyValue<T> next() throws Exception {
                while ((valuesInFileIt == null || !valuesInFileIt.hasNext())) {
                    Pair<FileBucket, FileInfo> next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        FileBucket bucket = next.getFirst();
                        FileInfo file = next.getSecond();
                        List<Pair<Long, T>> sortedEntries = readValuesWithCheck(bucket, file);
                        bucket.unlockRead();
                        valuesInFileIt = sortedEntries.iterator();
                    } else {
                        valuesInFileIt = null;
                        break;
                    }
                }
                if (valuesInFileIt != null && valuesInFileIt.hasNext()) {
                    Pair<Long, T> next = valuesInFileIt.next();
                    return new KeyValue<>(next.getFirst(), next.getSecond());
                } else {
                    return null;
                }
            }

        });
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        timeOfLastRead = System.currentTimeMillis();
        final FileIterator fileIterator = createFileIterator();
        return IterableUtils.iterator(new SimpleIterator<Long>() {

            private Iterator<Long> keysInFileIt;

            @Override
            public Long next() throws Exception {
                while ((keysInFileIt == null || !keysInFileIt.hasNext())) {
                    Pair<FileBucket, FileInfo> next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        FileBucket bucket = next.getFirst();
                        FileInfo file = next.getSecond();
                        List<Long> sortedKeys = readKeys(bucket, file);
                        bucket.unlockRead();
                        keysInFileIt = sortedKeys.iterator();
                    } else {
                        keysInFileIt = null;
                        break;
                    }
                }
                if (keysInFileIt != null && keysInFileIt.hasNext()) {
                    return keysInFileIt.next();
                } else {
                    return null;
                }
            }

        });
    }

    @Override
    public synchronized void freeMemory() {
        doActionIfNotClosed(new ActionIfNotClosed() {
            @Override
            public void doAction() {
                for (FileBucket bucket : fileBuckets) {
                    if (bucket.tryLockRead()) {
                        for (FileInfo fileInfo : bucket.getFiles()) {
                            long bytesReleased = fileInfo.discardFileContents();
                            updateSizeOfCachedFileContents(-bytesReleased);
                        }
                        bucket.unlockRead();
                    }
                }
            }
        });
    }

    @Override
    public String getMemoryUsage() {
        long totalUsed = 0;
        for (FileBucket bucket : fileBuckets) {
            bucket.lockRead();
            for (FileInfo fileInfo : bucket.getFiles()) {
                byte[] cachedContents = fileInfo.getCachedFileContents();
                if (cachedContents != null) {
                    totalUsed += cachedContents.length;
                }
            }
            bucket.unlockRead();
        }
        return "cached file contents " + totalUsed;
    }

    @Override
    public long apprSize() {
        int numOfFilesToSample = 100;
        long numOfObjects = 0;
        long sizeOfSampledFiles = 0;
        int numOfSampledFiles = 0;
        long sizeOfAllFiles = 0;
        try {
            FileIterator fileIt = createFileIterator();
            Pair<FileBucket, FileInfo> next = fileIt.lockCurrentBucketAndGetNextFile();
            while (next != null) {
                FileBucket bucket = next.getFirst();
                FileInfo file = next.getSecond();
                long fileSize = file.getSize();
                if (numOfSampledFiles < numOfFilesToSample) {
                    List<Long> keys = readKeys(bucket, file);
                    numOfObjects += keys.size();
                    sizeOfSampledFiles += fileSize;
                    if (fileSize == 0 && !keys.isEmpty()) {
                        UI.writeError("Something is wrong with file " + file.getFirstKey());
                    }
                    numOfSampledFiles++;
                }
                bucket.unlockRead();
                sizeOfAllFiles += fileSize;
                next = fileIt.lockCurrentBucketAndGetNextFile();
            }
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
        //always flushed
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

    @Override
    protected synchronized void doClose() {
        try {
            writeCleanFilesListIfNecessary();
        } finally {
            fileBuckets = null;
        }
    }

    private void updateSizeOfCachedFileContents(long byteDiff) {
        synchronized (sizeOfCachedFileContentsLock) {
            currentSizeOfCachedFileContents += byteDiff;
        }
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
                writeValue(tmpOutputStream, key, value);
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
        } catch (Exception exp) {
            try {
                close();
            } catch (Exception exp2) {
                //OK
            }
            throw new RuntimeException("Unexpected exception while rewriting file " + toFile(file).getAbsolutePath() + ". Closed this data interface", exp);
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

    private long writeValue(DataOutputStream dos, long key, T value) throws IOException {
        dos.writeLong(key);
        byte[] objectAsBytes = SerializationUtils.objectToBytesCheckForNull(value, getObjectClass());
        if (sizeOfValues == -1) {
            dos.writeInt(objectAsBytes.length);
            dos.write(objectAsBytes);
            return 8 + 4 + objectAsBytes.length;
        } else {
            dos.write(objectAsBytes);
            return 8 + sizeOfValues;
        }
    }

    private ReadValue<T> readValue(byte[] buffer, int position) throws IOException {
        int lengthOfObject;
        int lenghtOfLengthValue;
        if (sizeOfValues == -1) {
            lengthOfObject = SerializationUtils.bytesToInt(buffer, position);
            lenghtOfLengthValue = INT_SIZE;
        } else {
            lengthOfObject = sizeOfValues;
            lenghtOfLengthValue = 0;
        }
        T value = SerializationUtils.bytesToObjectCheckForNull(buffer, position + lenghtOfLengthValue, lengthOfObject, getObjectClass());
        return new ReadValue<>(lengthOfObject + lenghtOfLengthValue, value);
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
            } catch (Exception exp) {
                UI.writeError("Received exception while reading " + cleanFilesFile.getAbsolutePath(), exp);
            }
        }
        return result;
    }

    public synchronized void writeCleanFilesListIfNecessary() {
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
        File outputFile = new File(directory, CLEAN_FILES_FILE);
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(outputFile));
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
        } catch (Exception exp) {
            throw new RuntimeException("Received exception while writing list of clean files to " + outputFile.getAbsolutePath(), exp);
        }
    }

    private FileBucket getBucket(long key) {
        int ind = (int) ((key >> BITS_TO_DISCARD_FOR_FILE_BUCKETS) + fileBuckets.length / 2);
        return fileBuckets[ind];
    }

    private T readSingleValue(long keyToRead, FileInfo file) throws IOException {
        //See whether we can skip a portion of the file?
        int pos = file.getFileLocations() != null ? Collections.binarySearch((List) file.getFileLocations(), keyToRead) : -1;
        int startPos;
        if (pos == -1) {
            //Before first key, value can not be in file
            return null;
        } else {
            if (pos < 0) {
                pos = -(pos + 1);
            }
            if (pos == file.getFileLocations().size() || file.getFileLocations().get(pos).getFirst() > keyToRead) {
                pos--;
            }
            startPos = file.getFileLocations().get(pos).getSecond();
        }
        int endPos = pos + 1 < file.getFileLocations().size() ? file.getFileLocations().get(pos + 1).getSecond() : file.getSize();
        ReadBuffer readBuffer = getReadBuffer(file, startPos, endPos);
        startPos -= readBuffer.getOffset();
        endPos -= readBuffer.getOffset();
        byte firstByteOfKeyToRead = (byte) (keyToRead >> 56);
        byte[] buffer = readBuffer.getBuffer();
        int position = startPos;
        while (position < endPos) {
            byte currentByte = buffer[position];
            if (currentByte == firstByteOfKeyToRead) {
                long key = SerializationUtils.bytesToLong(buffer, position);
                position += LONG_SIZE;
                if (key == keyToRead) {
                    ReadValue<T> readValue = readValue(buffer, position);
                    return readValue.getValue();
                } else if (key > keyToRead) {
                    return null;
                } else {
                    //skip value
                    position += skipValue(buffer, position);
                }
            } else if (currentByte > firstByteOfKeyToRead) {
                //key too large, value not in this file
                return null;
            } else if (currentByte < firstByteOfKeyToRead) {
                //key too small, skip key and value
                position += LONG_SIZE;
                position += skipValue(buffer, position);
            }
        }
        return null;
    }

    private ReadBuffer getReadBuffer(FileInfo file, int requestedStartPos, int requestedEndPos) throws IOException {
        byte[] fileContents = file.getCachedFileContents();
        if (fileContents == null) {
            if (memoryManager.getMemoryStatus() == MemoryStatus.FREE && currentSizeOfCachedFileContents < maxSizeOfCachedFileContents) {
                //cache file contents. Lock on file object to make sure we don't read the content in parallel (this messes up the currentSizeOfCachedFileContents variable and is not very efficient)
                synchronized (file) {
                    fileContents = file.getCachedFileContents();
                    if (fileContents == null) {
                        fileContents = new byte[file.getSize()];
                        FileInputStream fis = new FileInputStream(toFile(file));
                        int bytesRead = fis.read(fileContents);
                        if (bytesRead != file.getSize()) {
                            throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.getSize() + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
                        }
                        updateSizeOfCachedFileContents(fileContents.length);
                        IOUtils.closeQuietly(fis);
                    }
                    file.setCachedFileContents(fileContents);
                }
                return new ReadBuffer(fileContents, 0);
            } else {
                FileInputStream fis = new FileInputStream(toFile(file));
                long bytesSkipped = fis.skip(requestedStartPos);
                if (bytesSkipped != requestedStartPos) {
                    throw new RuntimeException("Skipped " + bytesSkipped + " bytes, while we expected to skip " + requestedStartPos + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
                }
                byte[] buffer = new byte[requestedEndPos - requestedStartPos];
                int bytesRead = fis.read(buffer);
                if (bytesRead != buffer.length) {
                    throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.getSize() + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
                }
                IOUtils.closeQuietly(fis);
                return new ReadBuffer(buffer, requestedStartPos);
            }
        } else {
            if (fileContents.length != file.getSize()) {
                throw new RuntimeException("Buffer and file size don't match!");
            }
            return new ReadBuffer(fileContents, 0);
        }
    }

    private int skipValue(byte[] buffer, int position) throws IOException {
        //Skip some bytes
        Class<T> objectClass = getObjectClass();
        if (objectClass == Long.class || objectClass == Double.class) {
            return LONG_SIZE;
        } else if (objectClass == Integer.class || objectClass == Float.class) {
            return INT_SIZE;
        } else {
            int length = SerializationUtils.bytesToInt(buffer, position);
            return INT_SIZE + length;
        }
    }

    private DataOutputStream getOutputStream(FileInfo fileInfo, boolean append) throws FileNotFoundException {
        long bytesReleased = fileInfo.discardFileContents();
        updateSizeOfCachedFileContents(-bytesReleased);
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
            byte[] buffer = getReadBuffer(file, 0, file.getSize()).getBuffer();
            if (file.getSize() > 0) {
                int expectedNumberOfLongValues = file.getSize() / 16;
                List<Pair<Long, T>> result = new ArrayList<>(expectedNumberOfLongValues);
                if (!file.isDirty()) {
                    int position = 0;
                    while (position < buffer.length) {
                        long key = SerializationUtils.bytesToLong(buffer, position);
                        position += LONG_SIZE;
                        ReadValue<T> readValue = readValue(buffer, position);
                        position += readValue.getSize();
                        result.add(new Pair<>(key, readValue.getValue()));
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
                    int position = 0;
                    while (position < buffer.length) {
                        long key = SerializationUtils.bytesToLong(buffer, position);
                        position += LONG_SIZE;
                        ReadValue<T> readValue = readValue(buffer, position);
                        position += readValue.getSize();
                        int bucketInd = (int) ((key - start) / density);
                        if (bucketInd == buckets.length) {
                            bucketInd--; //rounding error?
                        }
                        if (bucketInd >= buckets.length) {
                            UI.write("huh?");
                        }
                        buckets[bucketInd].add(new Pair<>(key, readValue.getValue()));
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
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception while reading values from file " + toFile(file).getAbsolutePath(), ex);
        }
    }

    private FileIterator createFileIterator() {
        return new FileIterator();
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
        byte[] buffer = getReadBuffer(file, 0, file.getSize()).getBuffer();
        int position = 0;
        while (position < buffer.length) {
            result.add(SerializationUtils.bytesToLong(buffer, position));
            position += LONG_SIZE;
            position += skipValue(buffer, position);
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
        List<Pair<Long, Integer>> result = new ArrayList<>(fileLocations.size() / invSampleRate);
        for (int i = 0; i < fileLocations.size(); i++) {
            if (i % invSampleRate == 0) {
                result.add(fileLocations.get(i));
            }
        }
        return result;
    }

    @Override
    public void doOccasionalAction() {
        super.doOccasionalAction();
        doActionIfNotClosed(new ActionIfNotClosed() {
            @Override
            public void doAction() {
                writeCleanFilesListIfNecessary();
                checkLock();
            }
        });
    }

    private void checkLock() {
        File lockFile = new File(directory, LOCK_FILE);
        try {
            DataInputStream dis = new DataInputStream(new FileInputStream(lockFile));
            long id = dis.readLong();
            IOUtils.closeQuietly(dis);
            if (randomId != id) {
                writeLockFile(new Random().nextLong()); //try to notify other data interface that something is fucked up
                UI.writeError("The lock in " + lockFile.getAbsolutePath() + " was obtained by another data interface! Closing data interface. This will probably cause a lot of other errors...");
                close();
            }
        } catch (Exception exp) {
            throw new RuntimeException("Unexpected exception while trying to read lock file " + lockFile.getAbsolutePath());
        }
    }

    private static class ReadBuffer {
        private final byte[] buffer;
        private final int offset;

        private ReadBuffer(byte[] buffer, int offset) {
            this.buffer = buffer;
            this.offset = offset;
        }

        public byte[] getBuffer() {
            return buffer;
        }

        public int getOffset() {
            return offset;
        }
    }

    private static class ReadValue<T> {
        private int size;
        private T value;

        private ReadValue(int size, T value) {
            this.size = size;
            this.value = value;
        }

        public int getSize() {
            return size;
        }

        public T getValue() {
            return value;
        }
    }

    private class FileIterator {

        private int currentBucketInd = 0;
        private int fileInd = 0;

        public Pair<FileBucket, FileInfo> lockCurrentBucketAndGetNextFile() {
            if (currentBucketInd < fileBuckets.length) {
                FileBucket bucket = fileBuckets[currentBucketInd];
                bucket.lockRead();
                while (currentBucketInd < fileBuckets.length && fileInd >= bucket.getFiles().size()) {
                    fileInd = 0;
                    bucket.unlockRead();
                    currentBucketInd++;
                    if (currentBucketInd < fileBuckets.length) {
                        bucket = fileBuckets[currentBucketInd];
                        bucket.lockRead();
                    }
                }
                if (currentBucketInd < fileBuckets.length) {
                    return new Pair<>(bucket, bucket.getFiles().get(fileInd++));
                }
            }
            return null;
        }

    }
}
