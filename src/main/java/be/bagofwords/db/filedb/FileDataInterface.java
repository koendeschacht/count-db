package be.bagofwords.db.filedb;

import be.bagofwords.application.memory.MemoryGobbler;
import be.bagofwords.application.memory.MemoryManager;
import be.bagofwords.application.memory.MemoryStatus;
import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.DBUtils;
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
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class FileDataInterface<T extends Object> extends CoreDataInterface<T> implements MemoryGobbler {

    private static final long MAX_FILE_SIZE_WRITE = 50 * 1024 * 1024;
    private static final long MAX_FILE_SIZE_READ = 10 * 1024 * 1024;
    private static final long BITS_TO_DISCARD_FOR_FILE_BUCKETS = 58;
    private static final int BATCH_SIZE_PRIMITIVE_VALUES = 1000000;
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
    private final long maxSizeOfCachedFileContents = Runtime.getRuntime().maxMemory() / 3;
    private long currentSizeOfCachedFileContents;

    private long timeOfLastWrite;
    private long timeOfLastRead;
    private long timeOfLastRewrite;

    public FileDataInterface(MemoryManager memoryManager, Combinator<T> combinator, Class<T> objectClass, String directory, String nameOfSubset, boolean isTemporaryDataInterface) {
        super(nameOfSubset, objectClass, combinator, isTemporaryDataInterface);
        this.directory = new File(directory, nameOfSubset);
        this.sizeOfValues = SerializationUtils.getWidth(objectClass);
        this.randomId = new Random().nextLong();
        this.memoryManager = memoryManager;
        timeOfLastRead = 0;
        checkDataDir();
        MetaFile metaFile = readMetaInfo();
        initializeFileBuckets();
        initializeFiles(metaFile);
        if (metaFile != null) {
            timeOfLastRewrite = timeOfLastWrite = metaFile.getLastWrite();
        } else {
            timeOfLastRewrite = timeOfLastWrite = 0;
        }
        writeLockFile(randomId);
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
        bucket.lockAppend();
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
            bucket.unlockAppend();
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
                bucket.lockAppend();
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
                    bucket.unlockAppend();
                }
            }
            if (totalSizeWrittenInBatch > 0) {
                batchSize = BATCH_SIZE_PRIMITIVE_VALUES * 8 * batchSize / totalSizeWrittenInBatch;
            }
        }
    }

    private long getBatchSize() {
        return SerializationUtils.getWidth(getObjectClass()) == -1 ? BATCH_SIZE_NON_PRIMITIVE_VALUES : BATCH_SIZE_PRIMITIVE_VALUES;
    }

    @Override
    public CloseableIterator<KeyValue<T>> iterator(final Iterator<Long> keyIterator) {
        timeOfLastRead = System.currentTimeMillis();
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
                    FileBucket currentBucket = getBucket(key);
                    currentBucket.lockRead();
                    FileInfo file = currentBucket.getFile(key);
                    if (file != currentFile) {
                        currentFile = file;
                        valuesInCurrentFile = readMap(file);
                    }
                    currentBucket.unlockRead();
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
        timeOfLastRead = System.currentTimeMillis();
        final FileIterator fileIterator = createFileIterator();
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
    public synchronized void freeMemory() {
        doActionIfNotClosed(() -> {
            for (FileBucket bucket : fileBuckets) {
                bucket.lockRead();
                for (FileInfo fileInfo : bucket.getFiles()) {
                    long bytesReleased = fileInfo.discardFileContents();
                    updateSizeOfCachedFileContents(-bytesReleased);
                }
                bucket.unlockRead();
            }
        });
    }

    @Override
    public String getMemoryUsage() {
        return "cached file contents " + currentSizeOfCachedFileContents;
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
                long fileSize = file.getReadSize();
                if (numOfSampledFiles < numOfFilesToSample) {
                    List<Long> keys = readKeys(file);
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
    public synchronized void flush() {
        doActionIfNotClosed(() -> {
            rewriteAllFiles(false);
            checkLock();
        });
    }

    private synchronized void rewriteAllFiles(boolean alwaysWriteMetaFile) {
        final MutableLong numOfFilesRewritten = new MutableLong();
        final MutableLong numOfFilesDeleted = new MutableLong();

        if (timeOfLastWrite >= timeOfLastRewrite) {
            fileBuckets.parallelStream().forEach(bucket -> {
                bucket.lockAppend();
                try {
                    List<FileInfo> newFileList = new ArrayList<>(bucket.getFiles());
                    List<RewrittenFile> rewrittenFiles = new ArrayList<>();
                    List<FileInfo> filesToDelete = new ArrayList<>();
                    rewriteFiles(newFileList, rewrittenFiles, filesToDelete);
                    bucket.lockRewrite();
                    synchronized (numOfFilesRewritten) {
                        numOfFilesRewritten.setValue(numOfFilesRewritten.longValue() + rewrittenFiles.size());
                    }
                    synchronized (numOfFilesDeleted) {
                        numOfFilesDeleted.setValue(numOfFilesDeleted.longValue() + filesToDelete.size());
                    }
                    for (FileInfo file : filesToDelete) {
                        deleteFile(file);
                    }
                    for (RewrittenFile rewrittenFile : rewrittenFiles) {
                        swapTempForReal(rewrittenFile.getFile());
                        rewrittenFile.getFile().fileWasRewritten(rewrittenFile.getFileLocations(), rewrittenFile.getNewSize(), rewrittenFile.getNewSize());
                    }
                    bucket.setFiles(newFileList);
                    bucket.unlockRewrite();
                } catch (Exception exp) {
                    UI.writeError("Unexpected exception while rewriting files", exp);
                    throw new RuntimeException("Unexpected exception while rewriting files", exp);
                }
                bucket.unlockAppend();
            });
        }
        if (numOfFilesDeleted.getValue() > 0 || numOfFilesRewritten.getValue() > 0 || alwaysWriteMetaFile) {
            writeMetaFile();
        }
//        if (numOfFilesDeleted.getValue() > 0 || numOfFilesRewritten.getValue() > 0) {
//            UI.write("Rewritten " + numOfFilesRewritten + ", deleted " + numOfFilesDeleted + " files for " + getName());
//        }
        timeOfLastRewrite = System.currentTimeMillis();
    }


    /**
     * Do rewrites, but write all changes to temporary files, and don't modify the file bucket or the fileInfo's so reads can continue during this method
     */

    private void rewriteFiles(List<FileInfo> newFileList, List<RewrittenFile> rewrittenFiles, List<FileInfo> filesToDelete) throws IOException {
        for (int fileInd = 0; fileInd < newFileList.size(); fileInd++) {
            FileInfo file = newFileList.get(fileInd);
            boolean needsRewrite;
            long targetSize;
            if (inWritePhase()) {
                needsRewrite = file.isDirty() || file.getWriteSize() > MAX_FILE_SIZE_WRITE;
                targetSize = MAX_FILE_SIZE_WRITE / 4;
            } else {
                needsRewrite = file.isDirty() || file.getReadSize() > MAX_FILE_SIZE_READ;
                targetSize = MAX_FILE_SIZE_READ;
            }
            if (needsRewrite) {
                List<KeyValue<T>> values = readAllValues(file);
                int filesMergedWithThisFile = inWritePhase() ? 0 : mergeFileIfTooSmall(newFileList, fileInd, file.getWriteSize(), targetSize, values, filesToDelete);
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
                        rewrittenFiles.add(new RewrittenFile(file, currentSizeOfFile, sample(fileLocations, 50)));
                        fileLocations = new ArrayList<>();
                        file = new FileInfo(key, 0, 0);
                        currentSizeOfFile = 0;
                        newFileList.add(fileInd + 1, file);
                        fileInd++;
                        dos = getOutputStreamToTempFile(file);
                    }
                    fileLocations.add(new Pair<>(key, currentSizeOfFile));
                    dos.write(dataToWrite);
                    currentSizeOfFile += dataToWrite.length;
                }
                rewrittenFiles.add(new RewrittenFile(file, currentSizeOfFile, sample(fileLocations, 50)));
                dos.close();

            }
        }
    }

    private void deleteFile(FileInfo file) {
        boolean success = toFile(file).delete();
        if (!success) {
            throw new RuntimeException("Failed to delete file " + toFile(file).getAbsolutePath());
        }
    }

    private void dataWasWritten() {
        timeOfLastWrite = System.currentTimeMillis();
    }

    private void dataWasRead() {
        timeOfLastRead = System.currentTimeMillis();
    }

    private boolean inReadPhase() {
        return System.currentTimeMillis() - timeOfLastRead < 1000;
    }

    private boolean inWritePhase() {
        return System.currentTimeMillis() - timeOfLastWrite < 1000;
    }

    @Override
    public void optimizeForReading() {
        timeOfLastRead = System.currentTimeMillis(); //make sure we are in 'read' phase
        rewriteAllFiles(false);
    }

    @Override
    protected synchronized void doClose() {
        fileBuckets = null;
    }

    private void updateSizeOfCachedFileContents(long byteDiff) {
        synchronized (sizeOfCachedFileContentsLock) {
            currentSizeOfCachedFileContents += byteDiff;
        }
    }

    @Override
    public void dropAllData() {
        writeLockAllBuckets();
        for (FileBucket bucket : fileBuckets) {
            for (FileInfo file : bucket.getFiles()) {
                deleteFile(file);
            }
            bucket.getFiles().clear();
        }
        writeMetaFile();
        initializeFiles(null);
        writeUnlockAllBuckets();
    }

    private void writeLockAllBuckets() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.lockRewrite();
        }
    }

    private void writeUnlockAllBuckets() {
        for (FileBucket fileBucket : fileBuckets) {
            fileBucket.unlockRewrite();
        }
    }

    private void swapTempForReal(FileInfo file) throws IOException {
        long releasedBytes = file.discardFileContents();
        updateSizeOfCachedFileContents(-releasedBytes);
        Files.move(toTempFile(file).toPath(), toFile(file).toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private int mergeFileIfTooSmall(List<FileInfo> fileList, int currentFileInd, long combinedSize, long maxFileSize, List<KeyValue<T>> values, List<FileInfo> filesToDelete) {
        int nextFileInd = currentFileInd + 1;
        while (nextFileInd < fileList.size() && combinedSize + fileList.get(nextFileInd).getWriteSize() < maxFileSize) {
            //Combine the files
            FileInfo nextFile = fileList.remove(nextFileInd);
            values.addAll(readAllValues(nextFile));
            combinedSize += nextFile.getWriteSize();
            filesToDelete.add(nextFile);
            nextFileInd++;
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

    private void initializeFileBuckets() {
        fileBuckets = new ArrayList<>(1 << (64 - BITS_TO_DISCARD_FOR_FILE_BUCKETS));
        long start = Long.MIN_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        long end = Long.MAX_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        for (long val = start; val <= end; val++) {
            long firstKey = val << BITS_TO_DISCARD_FOR_FILE_BUCKETS;
            long lastKey = ((val + 1) << BITS_TO_DISCARD_FOR_FILE_BUCKETS) - 1;
            if (lastKey < firstKey) {
                //overflow
                lastKey = Long.MAX_VALUE;
            }
            fileBuckets.add(new FileBucket(firstKey, lastKey));
        }
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
        boolean foundMetaInformationOfAllFiles = true;
        for (String file : this.directory.list()) {
            if (file.matches("-?[0-9]+")) {
                long key = Long.parseLong(file);
                FileBucket bucket = getBucket(key);
                int size = sizeToInteger(new File(directory, file).length());
                FileInfo fileInfo = new FileInfo(key, 0, size);
                MetaFileInformation metaFileInformation = metaFile != null ? metaFile.getAllFilePositions().get(key) : null;
                if (metaFileInformation != null && metaFileInformation.getWriteLength() == size) {
                    fileInfo.fileWasRewritten(metaFileInformation.getKeyPositions(), metaFileInformation.getReadLength(), size);
                } else {
                    foundMetaInformationOfAllFiles = false;
                }
                bucket.getFiles().add(fileInfo);
            }
        }
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
        if (!foundMetaInformationOfAllFiles) {
            UI.write("Did not find meta information for all files of " + getName() + ", will rewrite all files just to make sure...");
            rewriteAllFiles(true);
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
            } catch (IOException exp) {
                UI.writeError("Received exception while reading " + cleanFilesFile.getAbsolutePath(), exp);
            }
        }
        return null;
    }

    private synchronized void writeMetaFile() {
        File outputFile = new File(directory, META_FILE);
        try {
            Map<Long, MetaFileInformation> allFilePositions = new HashMap<>();
            for (FileBucket fileBucket : fileBuckets) {
                for (FileInfo fileInfo : fileBucket.getFiles()) {
                    if (!fileInfo.isDirty()) {
                        allFilePositions.put(fileInfo.getFirstKey(), new MetaFileInformation(toList(fileInfo.getFileLocationsKeys(), fileInfo.getFileLocationsValues()), fileInfo.getReadSize(), fileInfo.getWriteSize()));
                    }
                }
            }
            MetaFile metaFile = new MetaFile(allFilePositions, timeOfLastWrite);
            FileOutputStream fos = new FileOutputStream(outputFile);
            SerializationUtils.writeObject(metaFile, fos);
            IOUtils.closeQuietly(fos);
        } catch (Exception exp) {
            throw new RuntimeException("Received exception while writing list of clean files to " + outputFile.getAbsolutePath(), exp);
        }
    }

    private List<Pair<Long, Integer>> toList(long[] fileLocationsKeys, int[] fileLocationsValues) {
        List<Pair<Long, Integer>> result = new ArrayList<>();
        for (int i = 0; i < fileLocationsKeys.length; i++) {
            result.add(new Pair<>(fileLocationsKeys[i], fileLocationsValues[i]));
        }
        return result;
    }

    private FileBucket getBucket(long key) {
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

    private FileIterator createFileIterator() {
        return new FileIterator();
    }

    private List<Long> readKeys(FileInfo file) throws IOException {
        if (file.isDirty()) {
            List<KeyValue<T>> values = readCleanValues(file);
            List<Long> keys = new ArrayList<>();
            for (KeyValue<T> value : values) {
                keys.add(value.getKey());
            }
            return keys;
        } else {
            return readKeysFromCleanFile(file);
        }
    }

    private List<Long> readKeysFromCleanFile(FileInfo file) throws IOException {
        List<Long> result = new ArrayList<>();
        byte[] buffer = getReadBuffer(file, 0, file.getReadSize()).getBuffer();
        int position = 0;
        while (position < buffer.length) {
            result.add(SerializationUtils.bytesToLong(buffer, position));
            position += LONG_SIZE;
            position += skipValue(buffer, position);
        }
        return result;
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
            if (currentBucketInd < fileBuckets.size()) {
                FileBucket bucket = fileBuckets.get(currentBucketInd);
                bucket.lockRead();
                while (currentBucketInd < fileBuckets.size() && fileInd >= bucket.getFiles().size()) {
                    fileInd = 0;
                    bucket.unlockRead();
                    currentBucketInd++;
                    if (currentBucketInd < fileBuckets.size()) {
                        bucket = fileBuckets.get(currentBucketInd);
                        bucket.lockRead();
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
        private Map<Long, MetaFileInformation> allFilePositions;
        private long lastWrite;

        public MetaFile(Map<Long, MetaFileInformation> allFilePositions, long lastWrite) {
            this.allFilePositions = allFilePositions;
            this.lastWrite = lastWrite;
        }

        //Constructor used in serialization
        public MetaFile() {
        }

        public long getLastWrite() {
            return lastWrite;
        }

        public Map<Long, MetaFileInformation> getAllFilePositions() {
            return allFilePositions;
        }

        public void setAllFilePositions(Map<Long, MetaFileInformation> allFilePositions) {
            this.allFilePositions = allFilePositions;
        }

        public void setLastWrite(long lastWrite) {
            this.lastWrite = lastWrite;
        }
    }

    public static class MetaFileInformation {
        private List<Pair<Long, Integer>> keyPositions;
        private int readLength;
        private int writeLength;

        public MetaFileInformation(List<Pair<Long, Integer>> keyPositions, int readLength, int writeLength) {
            this.keyPositions = keyPositions;
            this.readLength = readLength;
            this.writeLength = writeLength;
        }

        public int getReadLength() {
            return readLength;
        }

        public int getWriteLength() {
            return writeLength;
        }

        public List<Pair<Long, Integer>> getKeyPositions() {
            return keyPositions;
        }

        //Constructor used in serialization
        public MetaFileInformation() {
        }

        public void setKeyPositions(List<Pair<Long, Integer>> keyPositions) {
            this.keyPositions = keyPositions;
        }

        public void setReadLength(int readLength) {
            this.readLength = readLength;
        }

        public void setWriteLength(int writeLength) {
            this.writeLength = writeLength;
        }
    }

    private static class RewrittenFile {
        private FileInfo file;
        private int newSize;
        private List<Pair<Long, Integer>> fileLocations;

        private RewrittenFile(FileInfo file, int newSize, List<Pair<Long, Integer>> fileLocations) {
            this.file = file;
            this.newSize = newSize;
            this.fileLocations = fileLocations;
        }

        public FileInfo getFile() {
            return file;
        }

        public int getNewSize() {
            return newSize;
        }

        public List<Pair<Long, Integer>> getFileLocations() {
            return fileLocations;
        }
    }
}
