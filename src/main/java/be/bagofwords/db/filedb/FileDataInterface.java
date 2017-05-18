package be.bagofwords.db.filedb;

import be.bagofwords.application.TaskSchedulerService;
import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.DBUtils;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.memory.MemoryGobbler;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.memory.MemoryStatus;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.Pair;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;

public class FileDataInterface<T extends Object> extends CoreDataInterface<T> implements MemoryGobbler {

    private static final long MAX_FILE_SIZE_WRITE = 50 * 1024 * 1024;
    private static final long MAX_FILE_SIZE_READ = 10 * 1024 * 1024;
    private static final long BITS_TO_DISCARD_FOR_FILE_BUCKETS = 58;
    private static final int BATCH_SIZE_PRIMITIVE_VALUES = 100000;
    private static final int BATCH_SIZE_NON_PRIMITIVE_VALUES = 100;

    private static final String META_FILE = "META_FILE";
    private static final String LOCK_FILE = "LOCK";

    private static final int LONG_SIZE = 8;
    private static final int INT_SIZE = 4;

    private MemoryManager memoryManager;
    private File directory;
    private List<FileBucket> fileBuckets;
    private final int sizeOfValues;
    private final long randomId;

    private final String sizeOfCachedFileContentsLock = new String("LOCK");
    private final long maxSizeOfCachedFileContents;
    private long currentSizeOfCachedFileContents;

    private long timeOfLastWrite;
    private long timeOfLastRead;

    private boolean metaFileOutOfSync;

    public FileDataInterface(MemoryManager memoryManager, Combinator<T> combinator, Class<T> objectClass, String directory, String nameOfSubset, boolean isTemporaryDataInterface, TaskSchedulerService taskScheduler) {
        super(nameOfSubset, objectClass, combinator, isTemporaryDataInterface);
        this.directory = new File(directory, nameOfSubset);
        this.sizeOfValues = SerializationUtils.getWidth(objectClass);
        this.randomId = new Random().nextLong();
        this.memoryManager = memoryManager;
        this.maxSizeOfCachedFileContents = memoryManager.getAvailableMemoryInBytes() / 3;
        timeOfLastRead = 0;
        checkDataDir();
        MetaFile metaFile = readMetaInfo();
        initializeFiles(metaFile);
        writeLockFile(randomId);
        currentSizeOfCachedFileContents = 0;
        taskScheduler.schedulePeriodicTask(() -> ifNotClosed(() -> {
            rewriteAllFiles(false);
            checkLock();
        }), 1000); //rewrite files that are too large
    }

    @Override
    public T read(long key) {
        FileBucket bucket = getBucket(key);
        lockForRead(bucket);
        FileInfo file = bucket.getFile(key);
        try {
            int startPos;
            int pos = Arrays.binarySearch(file.getFileLocationsKeys(), key);
            if (pos == -1) {
                //Before first key, value can not be in file
                return null;
            } else {
                if (pos < 0) {
                    pos = -(pos + 1);
                }
                if (pos == file.getFileLocationsKeys().length || file.getFileLocationsKeys()[pos] > key) {
                    pos--;
                }
                startPos = file.getFileLocationsValues()[pos];
            }
            int endPos = pos + 1 < file.getFileLocationsKeys().length ? file.getFileLocationsValues()[pos + 1] : file.getReadSize();
            ReadBuffer readBuffer = getReadBuffer(file, startPos, endPos);
            startPos -= readBuffer.getOffset();
            endPos -= readBuffer.getOffset();
            byte firstByteOfKeyToRead = (byte) (key >> 56);
            byte[] buffer = readBuffer.getBuffer();
            int position = startPos;
            while (position < endPos) {
                byte currentByte = buffer[position];
                if (currentByte == firstByteOfKeyToRead) {
                    long currentKey = SerializationUtils.bytesToLong(buffer, position);
                    position += LONG_SIZE;
                    if (currentKey == key) {
                        ReadValue<T> readValue = readValue(buffer, position);
                        return readValue.getValue();
                    } else if (currentKey > key) {
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
        } catch (Exception exp) {
            throw new RuntimeException("Error in file " + toFile(file).getAbsolutePath(), exp);
        } finally {
            dataWasRead();
            bucket.unlockRead();
        }
    }

    @Override
    public void write(long key, T value) {
        FileBucket bucket = getBucket(key);
        bucket.lockWrite();
        FileInfo file = bucket.getFile(key);
        try {
            DataOutputStream dos = getAppendingOutputStream(file);
            int extraSize = writeValue(dos, key, value);
            dos.close();
            file.increaseWriteSize(extraSize);
            dataWasWritten();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write value with key " + key + " to file " + toFile(file).getAbsolutePath(), e);
        } finally {
            bucket.unlockWrite();
        }
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        long batchSize = getBatchSize();
        while (entries.hasNext()) {
            MappedLists<FileBucket, KeyValue<T>> entriesToFileBuckets = new MappedLists<>();
            int numRead = 0;
            while (numRead < batchSize && entries.hasNext()) {
                KeyValue<T> curr = entries.next();
                FileBucket fileBucket = getBucket(curr.getKey());
                entriesToFileBuckets.get(fileBucket).add(curr);
                numRead++;
            }
            long totalSizeWrittenInBatch = 0;
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
                            DataOutputStream dos = getAppendingOutputStream(file);
                            for (KeyValue<T> value : valuesForFile) {
                                int extraSize = writeValue(dos, value.getKey(), value.getValue());
                                file.increaseWriteSize(extraSize);
                                totalSizeWrittenInBatch += extraSize;
                            }
                            dataWasWritten();
                            dos.close();
                        } catch (Exception exp) {
                            throw new RuntimeException("Failed to write multiple values to file " + toFile(file).getAbsolutePath(), exp);
                        }
                    }
                } finally {
                    bucket.unlockWrite();
                }
            }
            if (totalSizeWrittenInBatch > 0) {
                batchSize = BATCH_SIZE_PRIMITIVE_VALUES * 16 * batchSize / totalSizeWrittenInBatch;
            }
        }
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        return new CloseableIterator<KeyValue<T>>() {

            private Iterator<KeyValue<T>> currBatchIterator;

            {
                readNextBatch(); //constructor
            }

            private void readNextBatch() {
                long batchSize = getBatchSize();
                List<Long> keysInBatch = new ArrayList<>();
                while (keyIterator.hasNext() && keysInBatch.size() < batchSize) {
                    keysInBatch.add(keyIterator.next());
                }
                Collections.sort(keysInBatch);
                List<KeyValue<T>> valuesInBatch = new ArrayList<>();
                FileInfo currentFile = null;
                Map<Long, T> valuesInCurrentFile = null;
                for (Long key : keysInBatch) {
                    FileBucket bucket = getBucket(key);
                    lockForRead(bucket);
                    FileInfo file = bucket.getFile(key);
                    if (file != currentFile) {
                        currentFile = file;
                        valuesInCurrentFile = readMap(file);
                    }
                    bucket.unlockRead();
                    T value = valuesInCurrentFile.get(key);
                    if (value != null) {
                        valuesInBatch.add(new KeyValue<>(key, value));
                    }
                }
                currBatchIterator = valuesInBatch.iterator();
            }

            @Override
            protected void closeInt() {
                //ok
            }

            @Override
            public boolean hasNext() {
                return currBatchIterator.hasNext();
            }

            @Override
            public KeyValue<T> next() {
                KeyValue<T> next = currBatchIterator.next();
                if (!currBatchIterator.hasNext()) {
                    readNextBatch();
                }
                return next;
            }
        };
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator() {
        final FileIterator fileIterator = new FileIterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private Iterator<KeyValue<T>> valuesInFileIt;

            @Override
            public KeyValue<T> next() throws Exception {
                while ((valuesInFileIt == null || !valuesInFileIt.hasNext())) {
                    Pair<FileBucket, FileInfo> next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        FileBucket bucket = next.getFirst();
                        FileInfo file = next.getSecond();
                        List<KeyValue<T>> sortedEntries = readCleanValues(file);
                        bucket.unlockRead();
                        valuesInFileIt = sortedEntries.iterator();
                    } else {
                        valuesInFileIt = null;
                        break;
                    }
                }
                if (valuesInFileIt != null && valuesInFileIt.hasNext()) {
                    return valuesInFileIt.next();
                } else {
                    return null;
                }
            }

        });
    }

    @Override
    public CloseableIterator<Long> keyIterator() {
        final FileIterator fileIterator = new FileIterator();
        return IterableUtils.iterator(new SimpleIterator<Long>() {

            private Iterator<Long> keysInFileIt;

            @Override
            public Long next() throws Exception {
                while ((keysInFileIt == null || !keysInFileIt.hasNext())) {
                    Pair<FileBucket, FileInfo> next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        FileBucket bucket = next.getFirst();
                        FileInfo file = next.getSecond();
                        List<Long> sortedKeys = readKeys(file);
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
    public long freeMemory() {
        MutableLong totalBytesReleased = new MutableLong(0);
        ifNotClosed(() -> {
            for (FileBucket bucket : fileBuckets) {
                bucket.lockRead();
                for (FileInfo fileInfo : bucket.getFiles()) {
                    long bytesReleased = fileInfo.discardFileContents();
                    updateSizeOfCachedFileContents(-bytesReleased);
                    totalBytesReleased.add(bytesReleased);
                }
                bucket.unlockRead();
            }
        });
        return totalBytesReleased.longValue();
    }

    @Override
    public long getMemoryUsage() {
        return currentSizeOfCachedFileContents;
    }

    @Override
    public long apprSize() {
        int numOfFilesToSample = 100;
        long numOfObjects = 0;
        long sizeOfSampledFiles = 0;
        int numOfSampledFiles = 0;
        long sizeOfAllFiles = 0;
        try {
            FileIterator fileIt = new FileIterator();
            Pair<FileBucket, FileInfo> next = fileIt.lockCurrentBucketAndGetNextFile();
            while (next != null) {
                FileBucket bucket = next.getFirst();
                FileInfo file = next.getSecond();
                long fileSize = file.getReadSize();
                if (numOfSampledFiles < numOfFilesToSample) {
                    List<Long> keys = readKeys(file);
                    numOfObjects += keys.size();
                    sizeOfSampledFiles += fileSize;
                    if (fileSize == 0 && !keys.isEmpty()) {
                        Log.e("Something is wrong with file " + file.getFirstKey());
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
        updateShouldBeCleanedInfo();
    }

    @Override
    public void optimizeForReading() {
        rewriteAllFiles(true);
    }

    @Override
    protected void doClose() {
        updateShouldBeCleanedInfo();
        if (metaFileOutOfSync) {
            writeMetaFile();
        }
        fileBuckets = null;
    }

    @Override
    public void dropAllData() {
        writeLockAllBuckets();
        for (FileBucket bucket : fileBuckets) {
            for (FileInfo file : bucket.getFiles()) {
                deleteFile(file);
            }
            bucket.getFiles().clear();
            bucket.setShouldBeCleanedBeforeRead(false);
        }
        makeSureAllFileBucketsHaveAtLeastOneFile();
        writeUnlockAllBuckets();
        writeMetaFile();
    }

    private void updateShouldBeCleanedInfo() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.lockWrite();
            if (!allFilesClean(fileBucket)) {
                fileBucket.setShouldBeCleanedBeforeRead(true);
            }
            fileBucket.unlockWrite();
        }
    }

    private synchronized void rewriteAllFiles(boolean forceClean) {
        int numOfFilesRewritten = fileBuckets.parallelStream().collect(Collectors.summingInt(bucket -> rewriteBucket(bucket, forceClean)));
        if (metaFileOutOfSync) {
            writeMetaFile();
        }
        if (DBUtils.DEBUG && numOfFilesRewritten > 0) {
            Log.i("Rewritten " + numOfFilesRewritten + " files for " + getName());
        }
    }

    private int rewriteBucket(FileBucket bucket, boolean forceClean) {
        if (forceClean) {
            bucket.lockWrite();
        } else {
            boolean success = bucket.tryLockWrite();
            if (!success) {
                return 0; //will not clean bucket now but continue with other buckets, we'll be back soon.
            }
        }
        try {
            int numOfRewrittenFiles = 0;
            for (int fileInd = 0; fileInd < bucket.getFiles().size() && (!closeWasRequested() || forceClean); fileInd++) {
                FileInfo file = bucket.getFiles().get(fileInd);
                boolean needsRewrite;
                long targetSize;
                if (inReadPhase() || forceClean) {
                    //read phrase
                    needsRewrite = !file.isClean();
                    targetSize = MAX_FILE_SIZE_READ;
                } else {
                    //write phase
                    double probOfRewriteForSize = file.getWriteSize() * 4.0 / MAX_FILE_SIZE_WRITE - 3.0;
                    needsRewrite = !file.isClean() && Math.random() < probOfRewriteForSize;
                    targetSize = MAX_FILE_SIZE_READ;
                }
                if (needsRewrite) {
                    //                    Log.i("Will rewrite file " + file.getFirstKey() + " " + getName() + " clean=" + file.isClean() + " force=" + forceClean + " readSize=" + file.getReadSize() + " writeSize=" + file.getWriteSize() + " targetSize=" + targetSize);
                    List<KeyValue<T>> values = readAllValues(file);
                    int filesMergedWithThisFile = inWritePhase() ? 0 : mergeFileIfTooSmall(bucket.getFiles(), fileInd, file.getWriteSize(), targetSize, values);
                    DataOutputStream dos = getOutputStreamToTempFile(file);
                    List<Pair<Long, Integer>> fileLocations = new ArrayList<>();
                    int currentSizeOfFile = 0;
                    for (KeyValue<T> entry : values) {
                        long key = entry.getKey();
                        T value = entry.getValue();
                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
                        DataOutputStream tmpOutputStream = new DataOutputStream(bos);
                        writeValue(tmpOutputStream, key, value);
                        byte[] dataToWrite = bos.toByteArray();
                        if (currentSizeOfFile > 0 && currentSizeOfFile + dataToWrite.length > targetSize) {
                            //Create new file
                            if (filesMergedWithThisFile > 0) {
                                throw new RuntimeException("Something went wrong! Merged file and then created new file?");
                            }
                            dos.close();
                            swapTempForReal(file);
                            file.fileWasRewritten(sample(fileLocations, 100), currentSizeOfFile, currentSizeOfFile);
                            fileLocations = new ArrayList<>();
                            file = new FileInfo(key, 0, 0);
                            currentSizeOfFile = 0;
                            bucket.getFiles().add(fileInd + 1, file);
                            fileInd++;
                            dos = getOutputStreamToTempFile(file);
                        }
                        fileLocations.add(new Pair<>(key, currentSizeOfFile));
                        dos.write(dataToWrite);
                        currentSizeOfFile += dataToWrite.length;
                    }
                    swapTempForReal(file);
                    file.fileWasRewritten(sample(fileLocations, 100), currentSizeOfFile, currentSizeOfFile);
                    dos.close();
                    numOfRewrittenFiles++;
                }
            }
            boolean allFilesClean = allFilesClean(bucket);
            if (allFilesClean) {
                bucket.setShouldBeCleanedBeforeRead(false);
            }
            if (numOfRewrittenFiles > 0) {
                metaFileOutOfSync = true;
            }
            return numOfRewrittenFiles;
        } catch (Exception exp) {
            Log.e("Unexpected exception while rewriting files", exp);
            throw new RuntimeException("Unexpected exception while rewriting files", exp);
        } finally {
            bucket.unlockWrite();
        }
    }

    private boolean allFilesClean(FileBucket bucket) {
        boolean allFilesClean = true;
        for (FileInfo file : bucket.getFiles()) {
            allFilesClean &= file.isClean();
        }
        return allFilesClean;
    }

    private void deleteFile(FileInfo file) {
        boolean success = toFile(file).delete();
        if (!success) {
            throw new RuntimeException("Failed to delete file " + toFile(file).getAbsolutePath());
        }
    }

    private void dataWasWritten() {
        timeOfLastWrite = System.currentTimeMillis();
        metaFileOutOfSync = true;
    }

    private void dataWasRead() {
        timeOfLastRead = System.currentTimeMillis();
    }

    private boolean inReadPhase() {
        return !inWritePhase();
    }

    private boolean inWritePhase() {
        return timeOfLastWrite > timeOfLastRead && System.currentTimeMillis() - timeOfLastRead > 10 * 1000;
    }

    private void updateSizeOfCachedFileContents(long byteDiff) {
        synchronized (sizeOfCachedFileContentsLock) {
            currentSizeOfCachedFileContents += byteDiff;
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

    private void lockForRead(FileBucket bucket) {
        bucket.lockRead();
        while (bucket.shouldBeCleanedBeforeRead()) {
            bucket.unlockRead();
            rewriteBucket(bucket, true);
            bucket.lockRead();
        }
    }

    private void swapTempForReal(FileInfo file) throws IOException {
        synchronized (file) {
            long releasedBytes = file.discardFileContents();
            updateSizeOfCachedFileContents(-releasedBytes);
        }
        Files.move(toTempFile(file).toPath(), toFile(file).toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private int mergeFileIfTooSmall(List<FileInfo> fileList, int currentFileInd, long combinedSize, long maxFileSize, List<KeyValue<T>> values) {
        int nextFileInd = currentFileInd + 1;
        while (nextFileInd < fileList.size() && combinedSize + fileList.get(nextFileInd).getWriteSize() < maxFileSize) {
            //Combine the files
            FileInfo nextFile = fileList.remove(nextFileInd);
            values.addAll(readAllValues(nextFile));
            combinedSize += nextFile.getWriteSize();
            deleteFile(nextFile);
        }
        return nextFileInd - currentFileInd - 1;
    }

    private int writeValue(DataOutputStream dos, long key, T value) throws IOException {
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

    private List<FileBucket> createEmptyFileBuckets() {
        List<FileBucket> bucket = new ArrayList<>(1 << (64 - BITS_TO_DISCARD_FOR_FILE_BUCKETS));
        long start = Long.MIN_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        long end = Long.MAX_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        for (long val = start; val <= end; val++) {
            long firstKey = val << BITS_TO_DISCARD_FOR_FILE_BUCKETS;
            long lastKey = ((val + 1) << BITS_TO_DISCARD_FOR_FILE_BUCKETS) - 1;
            if (lastKey < firstKey) {
                //overflow
                lastKey = Long.MAX_VALUE;
            }
            bucket.add(new FileBucket(firstKey, lastKey));
        }
        return bucket;
    }

    private void checkDataDir() {
        if (!directory.exists()) {
            boolean success = directory.mkdirs();
            if (!success) {
                throw new RuntimeException("Failed to create directory " + directory.getAbsolutePath());
            }
        }
        if (directory.isFile()) {
            throw new IllegalArgumentException("File should be directory but is file! " + directory.getAbsolutePath());
        }
    }

    private void initializeFiles(MetaFile metaFile) {
        String[] filesInDir = this.directory.list();
        if (metaFile != null && metaFileUpToDate(metaFile, filesInDir)) {
            metaFileOutOfSync = false;
            timeOfLastRead = metaFile.getLastRead();
            timeOfLastWrite = metaFile.getLastWrite();
            fileBuckets = metaFile.getFileBuckets();
        } else {
            metaFileOutOfSync = true;
            timeOfLastRead = timeOfLastWrite = 0;
            fileBuckets = createEmptyFileBuckets();
            if (filesInDir.length > 0) {
                Log.i("Missing (up-to-date) meta information for " + getName() + " will reconstruct data structures from files found in directory.");
                updateBucketsFromFiles(filesInDir);
            }
            makeSureAllFileBucketsHaveAtLeastOneFile();
        }
    }

    private boolean metaFileUpToDate(MetaFile metaFile, String[] filesInDir) {
        for (String file : filesInDir) {
            if (file.matches("-?[0-9]+")) {
                long key = Long.parseLong(file);
                FileBucket bucket = getBucket(metaFile.getFileBuckets(), key);
                long sizeOnDisk = new File(directory, file).length();
                FileInfo fileInfo = bucket.getFile(key);
                if (fileInfo.getFirstKey() != key) {
                    return false; //the name of the file on disk should be equal to the first key
                }
                if (fileInfo.getWriteSize() != sizeOnDisk) {
                    return false; //the file write size should be equal to the size on disk
                }
                if (!fileInfo.isClean() && !bucket.shouldBeCleanedBeforeRead()) {
                    return false; //if the file is dirty, the bucket should be marked as 'shouldBeCleanedBeforeRead'
                }
            }
        }
        for (FileBucket fileBucket : metaFile.getFileBuckets()) {
            if (fileBucket.getFiles().isEmpty()) {
                return false; //every bucket should contain at least one file
            }
            if (fileBucket.getFirstKey() != fileBucket.getFiles().get(0).getFirstKey()) {
                return false; //the first key of the bucket should match the first key of the first file
            }
            for (int i = 0; i < fileBucket.getFiles().size() - 1; i++) {
                if (fileBucket.getFiles().get(i).getFirstKey() >= fileBucket.getFiles().get(i + 1).getFirstKey()) {
                    return false; //files should be sorted according to first key
                }
            }
        }
        return true; //all good!
    }

    private void updateBucketsFromFiles(String[] filesInDir) {
        for (String file : filesInDir) {
            if (file.matches("-?[0-9]+")) {
                long key = Long.parseLong(file);
                FileBucket bucket = getBucket(key);
                long sizeOnDisk = new File(directory, file).length();
                FileInfo fileInfo = new FileInfo(key, 0, (int) sizeOnDisk);
                bucket.getFiles().add(fileInfo);
                bucket.setShouldBeCleanedBeforeRead(bucket.shouldBeCleanedBeforeRead() || sizeOnDisk > 0);
            }
        }
    }

    private void makeSureAllFileBucketsHaveAtLeastOneFile() {
        for (FileBucket bucket : fileBuckets) {
            if (bucket.getFiles().isEmpty()) {
                //We need at least one file per bucket..
                FileInfo first = new FileInfo(bucket.getFirstKey(), 0, 0);
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

    private MetaFile readMetaInfo() {
        File cleanFilesFile = new File(directory, META_FILE);
        if (cleanFilesFile.exists()) {
            try {
                InputStream fis = new BufferedInputStream(new FileInputStream(cleanFilesFile));
                MetaFile result = SerializationUtils.readObject(MetaFile.class, fis);
                IOUtils.closeQuietly(fis);
                return result;
            } catch (Exception exp) {
                Log.e("Received exception while reading " + cleanFilesFile.getAbsolutePath(), exp);
            }
        }
        return null;
    }

    private synchronized void writeMetaFile() {
        readLockAllBuckets();
        metaFileOutOfSync = false;
        File outputFile = new File(directory, META_FILE);
        try {
            MetaFile metaFile = new MetaFile(fileBuckets, timeOfLastWrite, timeOfLastRead);
            FileOutputStream fos = new FileOutputStream(outputFile);
            SerializationUtils.writeObject(metaFile, fos);
            IOUtils.closeQuietly(fos);
        } catch (Exception exp) {
            metaFileOutOfSync = true;
            throw new RuntimeException("Received exception while writing list of clean files to " + outputFile.getAbsolutePath(), exp);
        } finally {
            readUnlockAllBuckets();
        }
    }

    private FileBucket getBucket(long key) {
        return getBucket(fileBuckets, key);
    }

    private FileBucket getBucket(List<FileBucket> fileBuckets, long key) {
        int ind = (int) ((key >> BITS_TO_DISCARD_FOR_FILE_BUCKETS) + fileBuckets.size() / 2);
        return fileBuckets.get(ind);
    }

    private ReadBuffer getReadBuffer(FileInfo file, int requestedStartPos, int requestedEndPos) throws IOException {
        byte[] fileContents = file.getCachedFileContents();
        if (fileContents == null) {
            if (memoryManager.getMemoryStatus() == MemoryStatus.FREE && currentSizeOfCachedFileContents < maxSizeOfCachedFileContents) {
                //cache file contents. Lock on file object to make sure we don't read the content in parallel (this messes up the currentSizeOfCachedFileContents variable and is not very efficient)
                synchronized (file) {
                    fileContents = file.getCachedFileContents();
                    if (fileContents == null) {
                        fileContents = new byte[file.getReadSize()];
                        FileInputStream fis = new FileInputStream(toFile(file));
                        int bytesRead = fis.read(fileContents);
                        if (bytesRead != file.getReadSize()) {
                            throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.getReadSize() + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
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
                    throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.getReadSize() + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
                }
                IOUtils.closeQuietly(fis);
                return new ReadBuffer(buffer, requestedStartPos);
            }
        } else {
            if (fileContents.length != file.getReadSize()) {
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

    private DataOutputStream getAppendingOutputStream(FileInfo fileInfo) throws FileNotFoundException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toFile(fileInfo), true)));
    }

    private DataOutputStream getOutputStreamToTempFile(FileInfo fileInfo) throws FileNotFoundException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toTempFile(fileInfo), false)));
    }

    private File toFile(FileInfo fileInfo) {
        if (directory == null) {
            throw new RuntimeException("Directory is null, probably the data interface was closed already!");
        }
        return new File(directory, Long.toString(fileInfo.getFirstKey()));
    }

    private File toTempFile(FileInfo fileInfo) {
        if (directory == null) {
            throw new RuntimeException("Directory is null, probably the data interface was closed already!");
        }
        return new File(directory, "tmp." + Long.toString(fileInfo.getFirstKey()));
    }

    private Map<Long, T> readMap(FileInfo file) {
        List<KeyValue<T>> values = readCleanValues(file);
        Map<Long, T> result = new HashMap<>(values.size());
        for (KeyValue<T> value : values) {
            result.put(value.getKey(), value.getValue());
        }
        return result;
    }

    private List<KeyValue<T>> readCleanValues(FileInfo file) {
        try {
            byte[] buffer = getReadBuffer(file, 0, file.getReadSize()).getBuffer();
            int expectedNumberOfValues = getLowerBoundOnNumberOfValues(file.getReadSize());
            List<KeyValue<T>> result = new ArrayList<>(expectedNumberOfValues);
            int position = 0;
            while (position < buffer.length) {
                long key = SerializationUtils.bytesToLong(buffer, position);
                position += LONG_SIZE;
                ReadValue<T> readValue = readValue(buffer, position);
                position += readValue.getSize();
                result.add(new KeyValue<>(key, readValue.getValue()));
            }
            dataWasRead();
            return result;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception while reading values from file " + toFile(file).getAbsolutePath(), ex);
        }
    }

    private List<KeyValue<T>> readAllValues(FileInfo file) {
        try {
            byte[] buffer = readCompleteFile(file);
            if (buffer.length > 0) {
                int expectedNumberOfValues = getLowerBoundOnNumberOfValues(file.getWriteSize());
                List<KeyValue<T>> result = new ArrayList<>(expectedNumberOfValues);
                //read values in buckets
                int numberOfBuckets = Math.max(1, expectedNumberOfValues / 1000);
                List[] buckets = new List[numberOfBuckets];
                for (int i = 0; i < buckets.length; i++) {
                    buckets[i] = new ArrayList(expectedNumberOfValues / numberOfBuckets);
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
                    buckets[bucketInd].add(new KeyValue<>(key, readValue.getValue()));
                }
                for (int bucketInd = 0; bucketInd < buckets.length; bucketInd++) {
                    List<KeyValue<T>> currentBucket = buckets[bucketInd];
                    DBUtils.mergeValues(result, currentBucket, getCombinator());
                    buckets[bucketInd] = null; //Free some memory
                }
                return result;
            } else {
                return Collections.emptyList();
            }
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception while reading values from file " + toFile(file).getAbsolutePath(), ex);
        }
    }

    private byte[] readCompleteFile(FileInfo file) throws IOException {
        FileInputStream fis = new FileInputStream(toFile(file));
        byte[] buffer = new byte[file.getWriteSize()];
        int bytesRead = fis.read(buffer);
        if (bytesRead != buffer.length) {
            if (!(buffer.length == 0 && bytesRead == -1)) {
                throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + buffer.length + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
            }
        }
        IOUtils.closeQuietly(fis);
        return buffer;
    }

    private int getLowerBoundOnNumberOfValues(int sizeOfFile) {
        int width = sizeOfValues;
        if (width == -1) {
            width = 4; //will probably be much larger...
        }
        return sizeOfFile / (8 + width);
    }

    private List<Long> readKeys(FileInfo file) throws IOException {
        List<Long> result = new ArrayList<>();
        byte[] buffer = getReadBuffer(file, 0, file.getReadSize()).getBuffer();
        int position = 0;
        while (position < buffer.length) {
            result.add(SerializationUtils.bytesToLong(buffer, position));
            position += LONG_SIZE;
            position += skipValue(buffer, position);
        }
        dataWasRead();
        return result;
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

    private void checkLock() {
        File lockFile = new File(directory, LOCK_FILE);
        try {
            DataInputStream dis = new DataInputStream(new FileInputStream(lockFile));
            long id = dis.readLong();
            IOUtils.closeQuietly(dis);
            if (randomId != id) {
                writeLockFile(new Random().nextLong()); //try to notify other data interface that something is fucked up
                Log.e("The lock in " + lockFile.getAbsolutePath() + " was obtained by another data interface! Closing data interface. This will probably cause a lot of other errors...");
                close();
            }
        } catch (Exception exp) {
            throw new RuntimeException("Unexpected exception while trying to read lock file " + lockFile.getAbsolutePath());
        }
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

    private long getBatchSize() {
        return SerializationUtils.getWidth(getObjectClass()) == -1 ? BATCH_SIZE_NON_PRIMITIVE_VALUES : BATCH_SIZE_PRIMITIVE_VALUES;
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
            if (currentBucketInd < fileBuckets.size()) {
                FileBucket bucket = fileBuckets.get(currentBucketInd);
                lockForRead(bucket);
                while (currentBucketInd < fileBuckets.size() && fileInd >= bucket.getFiles().size()) {
                    fileInd = 0;
                    bucket.unlockRead();
                    currentBucketInd++;
                    if (currentBucketInd < fileBuckets.size()) {
                        bucket = fileBuckets.get(currentBucketInd);
                        lockForRead(bucket);
                    }
                }
                if (currentBucketInd < fileBuckets.size()) {
                    return new Pair<>(bucket, bucket.getFiles().get(fileInd++));
                }
            }
            return null;
        }

    }

    public static class MetaFile {
        private List<FileBucket> fileBuckets;
        private long lastWrite;
        private long lastRead;

        public MetaFile(List<FileBucket> fileBuckets, long lastWrite, long lastRead) {
            this.fileBuckets = fileBuckets;
            this.lastRead = lastRead;
            this.lastWrite = lastWrite;
        }

        //Constructor used in serialization
        public MetaFile() {
        }

        public List<FileBucket> getFileBuckets() {
            return fileBuckets;
        }

        public void setFileBuckets(List<FileBucket> fileBuckets) {
            this.fileBuckets = fileBuckets;
        }

        public long getLastWrite() {
            return lastWrite;
        }

        public void setLastWrite(long lastWrite) {
            this.lastWrite = lastWrite;
        }

        public long getLastRead() {
            return lastRead;
        }

        public void setLastRead(long lastRead) {
            this.lastRead = lastRead;
        }
    }

}
