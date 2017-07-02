package be.bagofwords.db.newfiledb;

import be.bagofwords.db.CoreDataInterface;
import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.db.impl.DBUtils;
import be.bagofwords.db.methods.KeyFilter;
import be.bagofwords.db.methods.ObjectSerializer;
import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.iterator.IterableUtils;
import be.bagofwords.iterator.SimpleIterator;
import be.bagofwords.jobs.AsyncJobService;
import be.bagofwords.logging.Log;
import be.bagofwords.memory.MemoryGobbler;
import be.bagofwords.memory.MemoryManager;
import be.bagofwords.memory.MemoryStatus;
import be.bagofwords.util.KeyValue;
import be.bagofwords.util.MappedLists;
import be.bagofwords.util.Pair;
import be.bagofwords.util.SerializationUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

import static be.bagofwords.db.newfiledb.LockMethod.*;

public class NewFileDataInterface<T extends Object> extends CoreDataInterface<T> implements MemoryGobbler {

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
    private FileNode fileNodesRoot;
    private final int sizeOfValues;
    private final long randomId;

    private final String sizeOfCachedFileContentsLock = new String("LOCK");
    private final long maxSizeOfCachedFileContents;
    private long currentSizeOfCachedFileContents;

    private long timeOfLastWrite;
    private long timeOfLastRead;

    private boolean metaFileOutOfSync;

    private boolean closeWasRequested;

    public NewFileDataInterface(MemoryManager memoryManager, Combinator<T> combinator, Class<T> objectClass, ObjectSerializer<T> objectSerializer, String directory, String name, boolean isTemporaryDataInterface, AsyncJobService asyncJobService) {
        super(name, objectClass, combinator, objectSerializer, isTemporaryDataInterface);
        this.directory = new File(directory, name);
        this.sizeOfValues = SerializationUtils.getWidth(objectClass);
        this.randomId = new Random().nextLong();
        this.memoryManager = memoryManager;
        this.maxSizeOfCachedFileContents = memoryManager.getAvailableMemoryInBytes() / 3;
        timeOfLastRead = 0;
        checkDataDir();
        initializeFromMetaFile();
        writeLockFile(randomId);
        currentSizeOfCachedFileContents = 0;
        asyncJobService.schedulePeriodicJob(() -> ifNotClosed(() -> {
            rewriteAllFiles(false);
            checkLock();
        }), 1000); //rewrite files that are too large
    }

    private void initializeFromMetaFile() {
        MetaFile metaFile = readMetaInfo();
        String[] filesInDir = this.directory.list();
        if (metaFile != null && metaFileUpToDate(metaFile, filesInDir)) {
            metaFileOutOfSync = false;
            timeOfLastRead = metaFile.getLastRead();
            timeOfLastWrite = metaFile.getLastWrite();
            fileNodesRoot = metaFile.getFileNodesRoot();
        } else {
            metaFileOutOfSync = true;
            timeOfLastRead = timeOfLastWrite = 0;
            fileNodesRoot = createEmptyFileNodes();
            if (filesInDir.length > 0) {
                Log.i("Missing (up-to-date) meta information for " + getName() + " will reconstruct data structures from files found in directory.");
                updateBucketsFromFiles(filesInDir);
            }
            makeSureAllFileBucketsHaveAtLeastOneFile();
        }
    }

    @Override
    public T read(long key) {
        FileNode file = lockForRead(key);
        try {
            int startPos;
            int pos = Arrays.binarySearch(file.fileLocationsKeys, key);
            if (pos == -1) {
                //Before first key, value can not be in file
                return null;
            } else {
                if (pos < 0) {
                    pos = -(pos + 1);
                }
                if (pos == file.fileLocationsKeys.length || file.fileLocationsKeys[pos] > key) {
                    pos--;
                }
                startPos = file.fileLocationsValues[pos];
            }
            int endPos = pos + 1 < file.fileLocationsKeys.length ? file.fileLocationsValues[pos + 1] : file.readSize;
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
                        ReadValue<T> readValue = readValue(buffer, position, true);
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
            file.unlockRead();
        }
    }

    @Override
    public void write(long key, T value) {
        FileNode fileNode = getBucket(key, LOCK_WRITE);
        fileNode.lockWrite();
        try {
            DataOutputStream dos = getAppendingOutputStream(fileNode);
            int extraSize = writeValue(dos, key, value);
            dos.close();
            fileNode.increaseWriteSize(extraSize);
            dataWasWritten();
        } catch (Exception e) {
            throw new RuntimeException("Failed to write value with key " + key + " to file " + toFile(fileNode).getAbsolutePath(), e);
        } finally {
            fileNode.unlockWrite();
        }
    }

    @Override
    public void write(Iterator<KeyValue<T>> entries) {
        long batchSize = getBatchSize();
        while (entries.hasNext()) {
            MappedLists<FileNode, KeyValue<T>> entriesToFileBuckets = new MappedLists<>();
            int numRead = 0;
            while (numRead < batchSize && entries.hasNext()) {
                KeyValue<T> curr = entries.next();
                FileNode fileNode = getBucket(curr.getKey(), NO_LOCK);
                entriesToFileBuckets.get(fileNode).add(curr);
                numRead++;
            }
            long totalSizeWrittenInBatch = 0;
            for (FileNode fileNode : entriesToFileBuckets.keySet()) {
                List<KeyValue<T>> values = entriesToFileBuckets.get(fileNode);
                fileNode.lockWrite();
                try {
                    DataOutputStream dos = getAppendingOutputStream(fileNode);
                    for (KeyValue<T> value : values) {
                        int extraSize = writeValue(dos, value.getKey(), value.getValue());
                        fileNode.increaseWriteSize(extraSize);
                        totalSizeWrittenInBatch += extraSize;
                    }
                    dataWasWritten();
                    dos.close();
                } catch (Exception exp) {
                    throw new RuntimeException("Failed to write values to file " + toFile(fileNode).getAbsolutePath(), exp);
                } finally {
                    fileNode.unlockWrite();
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
                FileNode currentFile = null;
                Map<Long, T> valuesInCurrentFile = null;
                for (Long key : keysInBatch) {
                    FileNode file = getBucket(key, LOCK_READ);
                    if (file != currentFile) {
                        currentFile = file;
                        valuesInCurrentFile = readMap(file);
                    }
                    file.unlockRead();
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
                    FileNode next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        List<KeyValue<T>> sortedEntries = readCleanValues(next);
                        next.unlockRead();
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
    public CloseableIterator<KeyValue<T>> iterator(KeyFilter keyFilter) {
        final FileIterator fileIterator = new FileIterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private Iterator<KeyValue<T>> valuesInFileIt;

            @Override
            public KeyValue<T> next() throws Exception {
                while ((valuesInFileIt == null || !valuesInFileIt.hasNext())) {
                    FileNode next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        if (keyFilter.acceptKeysAboveOrEqual(next.firstKey) && keyFilter.acceptKeysBelow(next.lastKey)) {
                            List<KeyValue<T>> sortedEntries = readCleanValuesWithKeyFilter(next, keyFilter);
                            valuesInFileIt = sortedEntries.iterator();
                        }
                        next.unlockRead();
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
                    FileNode next = fileIterator.lockCurrentBucketAndGetNextFile();
                    if (next != null) {
                        List<Long> sortedKeys = readKeys(next);
                        next.unlockRead();
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
        ifNotClosed(() ->
                fileNodesRoot.doForAllLeafs(leaf -> {
                    leaf.lockRead();
                    long bytesReleased = leaf.discardFileContents();
                    updateSizeOfCachedFileContents(-bytesReleased);
                    totalBytesReleased.add(bytesReleased);
                }, LOCK_READ));
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
            FileNode file = fileIt.lockCurrentBucketAndGetNextFile();
            while (file != null) {
                long fileSize = file.readSize;
                if (numOfSampledFiles < numOfFilesToSample) {
                    List<Long> keys = readKeys(file);
                    numOfObjects += keys.size();
                    sizeOfSampledFiles += fileSize;
                    if (fileSize == 0 && !keys.isEmpty()) {
                        Log.e("Something is wrong with file " + file.firstKey);
                    }
                    numOfSampledFiles++;
                }
                file.unlockRead();
                sizeOfAllFiles += fileSize;
                file = fileIt.lockCurrentBucketAndGetNextFile();
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
    public void flushImpl() {
        updateShouldBeCleanedInfo();
    }

    @Override
    public void optimizeForReading() {
        rewriteAllFiles(true);
    }

    @Override
    protected void doClose() {
        closeWasRequested = true;
        updateShouldBeCleanedInfo();
        if (metaFileOutOfSync) {
            writeMetaFile();
        }
        fileNodesRoot = null;
    }

    @Override
    public void dropAllData() {
        writeLockAllBuckets();
        fileNodesRoot.doForAllLeafs((leaf) -> {
            deleteFile(leaf);
            leaf.setShouldBeCleanedBeforeRead(false);
        }, NO_LOCK);
        makeSureAllFileBucketsHaveAtLeastOneFile();
        writeUnlockAllBuckets();
        writeMetaFile();
    }

    @Override
    public long lastFlush() {
        return timeOfLastWrite;
    }

    private void updateShouldBeCleanedInfo() {
        fileNodesRoot.doForAllLeafs(leaf -> leaf.setShouldBeCleanedBeforeRead(leaf.readSize == leaf.writeSize), LOCK_WRITE);
    }

    private synchronized void rewriteAllFiles(boolean forceClean) {
        //TODO: do this in parallel?
        MutableInt numOfFilesRewritten = new MutableInt();
        fileNodesRoot.doForAllLeafs(leaf -> {
            int files = rewriteBucket(leaf, forceClean);
            numOfFilesRewritten.add(files);
        }, LOCK_WRITE);
        if (metaFileOutOfSync) {
            writeMetaFile();
        }
        if (DBUtils.DEBUG && numOfFilesRewritten.intValue() > 0) {
            Log.i("Rewritten " + numOfFilesRewritten.intValue() + " files for " + getName());
        }
    }

    private int rewriteBucket(FileNode file, boolean forceClean) {
        if (forceClean) {
            file.lockWrite();
        } else {
            boolean success = file.tryLockWrite();
            if (!success) {
                return 0; //will not clean bucket now but continue with other buckets, we'll be back soon.
            }
        }
        try {
            boolean needsRewrite;
            long targetSize;
            if (inReadPhase() || forceClean) {
                //read phrase
                needsRewrite = !file.isClean();
                targetSize = MAX_FILE_SIZE_READ;
            } else {
                //write phase
                double probOfRewriteForSize = file.writeSize * 4.0 / MAX_FILE_SIZE_WRITE - 3.0;
                needsRewrite = !file.isClean() && Math.random() < probOfRewriteForSize;
                targetSize = MAX_FILE_SIZE_READ;
            }
            if (needsRewrite) {
                //                    Log.i("Will rewrite file " + file.firstKey + " " + getName() + " clean=" + file.isClean() + " force=" + forceClean + " readSize=" + file.readSize + " writeSize=" + file.writeSize + " targetSize=" + targetSize);
                List<KeyValue<T>> values = readAllValues(file);
                int filesMergedWithThisFile = inWritePhase() ? 0 : mergeFileIfTooSmall(file, file.writeSize, targetSize, values);
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
                        FileNode newNode = new FileNode(key, file.lastKey);
                        file.setChildNodes(new FileNode(file.firstKey, key), newNode);
                        file = newNode;
                        currentSizeOfFile = 0;
                        dos = getOutputStreamToTempFile(file);
                    }
                    fileLocations.add(new Pair<>(key, currentSizeOfFile));
                    dos.write(dataToWrite);
                    currentSizeOfFile += dataToWrite.length;
                }
                swapTempForReal(file);
                file.fileWasRewritten(sample(fileLocations, 100), currentSizeOfFile, currentSizeOfFile);
                dos.close();
                file.setShouldBeCleanedBeforeRead(!file.isClean());
                metaFileOutOfSync = true;
                return 1;
            } else {
                return 0;
            }
        } catch (Exception exp) {
            Log.e("Unexpected exception while rewriting files", exp);
            throw new RuntimeException("Unexpected exception while rewriting files", exp);
        } finally {
            file.unlockWrite();
        }
    }

    private void deleteFile(FileNode file) {
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
        fileNodesRoot.doForAllLeafs((leaf) -> {
        }, LOCK_WRITE, false);
    }

    private void writeUnlockAllBuckets() {
        fileNodesRoot.doForAllLeafs((leaf) -> {
        }, LOCK_WRITE, true);
    }

    private void readLockAllBuckets() {
        fileNodesRoot.doForAllLeafs((leaf) -> {
        }, LOCK_READ, false);
    }

    private void readUnlockAllBuckets() {
        fileNodesRoot.doForAllLeafs((leaf) -> {
        }, LOCK_READ, true);
    }

    private FileNode lockForRead(long key) {
        FileNode node = getBucket(key, LOCK_READ);
        while (node.shouldBeCleanedBeforeRead()) {
            rewriteBucket(node, true);
        }
        return node;
    }

    private void swapTempForReal(FileNode file) throws IOException {
        synchronized (file) {
            long releasedBytes = file.discardFileContents();
            updateSizeOfCachedFileContents(-releasedBytes);
        }
        Files.move(toTempFile(file).toPath(), toFile(file).toPath(), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    }

    private int mergeFileIfTooSmall(FileNode fileNode, long combinedSize, long maxFileSize, List<KeyValue<T>> values) {
        if (fileNode.writeSize < maxFileSize) {
            Log.w("We should merge " + fileNode);
            return -1;
        } else {
            return 0;
        }
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

    private ReadValue<T> readValue(byte[] buffer, int position, boolean readActualValue) throws IOException {
        int lengthOfObject;
        int lenghtOfLengthValue;
        if (sizeOfValues == -1) {
            lengthOfObject = SerializationUtils.bytesToInt(buffer, position);
            lenghtOfLengthValue = INT_SIZE;
        } else {
            lengthOfObject = sizeOfValues;
            lenghtOfLengthValue = 0;
        }
        T value;
        if (readActualValue) {
            value = SerializationUtils.bytesToObjectCheckForNull(buffer, position + lenghtOfLengthValue, lengthOfObject, getObjectClass());
        } else {
            value = null;
        }
        return new ReadValue<>(lengthOfObject + lenghtOfLengthValue, value);
    }

    private FileNode createEmptyFileNodes() {
        FileNode root = new FileNode(Long.MIN_VALUE, Long.MAX_VALUE);
        addNodesRecursively(root, Long.MAX_VALUE / 8);
        return root;
    }

    private void addNodesRecursively(FileNode node, long maxInterval) {
        if ((double) node.lastKey - node.firstKey > maxInterval) {
            long split = node.firstKey + node.lastKey / 2 - node.firstKey / 2;
            node.setChildNodes(new FileNode(node.firstKey, split), new FileNode(split, node.lastKey));
            addNodesRecursively(node.getLeft(), maxInterval);
            addNodesRecursively(node.getRight(), maxInterval);
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

    private boolean metaFileUpToDate(MetaFile metaFile, String[] filesInDir) {
        Set<String> filesOnDisk = new HashSet<>();
        for (String file : filesInDir) {
            if (file.matches("-?[0-9]+")) {
                filesOnDisk.add(file);
            }
        }
        MutableBoolean upToDate = new MutableBoolean(true);
        fileNodesRoot.doForAllLeafs(leaf -> {
            if (!filesOnDisk.contains(leaf.fileName)) {
                upToDate.setFalse();
                return;
            }
            long sizeOnDisk = new File(directory, leaf.fileName).length();
            if (leaf.writeSize != sizeOnDisk) {
                upToDate.setFalse();
                return;
            }
            if (!leaf.isClean() && !leaf.shouldBeCleanedBeforeRead()) {
                upToDate.setFalse();
            }
        }, LOCK_READ);
        return upToDate.booleanValue();
    }

    private void updateBucketsFromFiles(String[] filesInDir) {
        throw new RuntimeException("Not yet implemented");
    }

    private void makeSureAllFileBucketsHaveAtLeastOneFile() {
        fileNodesRoot.doForAllLeafs(leaf -> {
            File file = toFile(leaf);
            try {
                if (!file.exists() && !file.createNewFile()) {
                    throw new RuntimeException("Failed to create file " + file.getAbsolutePath());
                }
            } catch (IOException exp) {
                throw new RuntimeException("Failed to create file " + file.getAbsolutePath(), exp);
            }
        }, LOCK_WRITE);
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
            MetaFile metaFile = new MetaFile(fileNodesRoot, timeOfLastWrite, timeOfLastRead);
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

    private FileNode getBucket(long key, LockMethod lockMethod) {
        return fileNodesRoot.getNode(key, lockMethod);
    }

    private ReadBuffer getReadBuffer(FileNode file, int requestedStartPos, int requestedEndPos) throws IOException {
        byte[] fileContents = file.cachedFileContents;
        if (fileContents == null) {
            if (memoryManager.getMemoryStatus() == MemoryStatus.FREE && currentSizeOfCachedFileContents < maxSizeOfCachedFileContents) {
                //cache file contents. Lock on file object to make sure we don't read the content in parallel (this messes up the currentSizeOfCachedFileContents variable and is not very efficient)
                synchronized (file) {
                    fileContents = file.cachedFileContents;
                    if (fileContents == null) {
                        fileContents = new byte[file.readSize];
                        FileInputStream fis = new FileInputStream(toFile(file));
                        int bytesRead = fis.read(fileContents);
                        if (bytesRead != file.readSize) {
                            throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.readSize + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
                        }
                        updateSizeOfCachedFileContents(fileContents.length);
                        IOUtils.closeQuietly(fis);
                    }
                    file.cachedFileContents = fileContents;
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
                    throw new RuntimeException("Read " + bytesRead + " bytes, while we expected " + file.readSize + " bytes in file " + toFile(file).getAbsolutePath() + " which currently has size " + toFile(file).length());
                }
                IOUtils.closeQuietly(fis);
                return new ReadBuffer(buffer, requestedStartPos);
            }
        } else {
            if (fileContents.length != file.readSize) {
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

    private DataOutputStream getAppendingOutputStream(FileNode fileInfo) throws FileNotFoundException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toFile(fileInfo), true)));
    }

    private DataOutputStream getOutputStreamToTempFile(FileNode fileInfo) throws FileNotFoundException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toTempFile(fileInfo), false)));
    }

    private File toFile(FileNode fileInfo) {
        if (directory == null) {
            throw new RuntimeException("Directory is null, probably the data interface was closed already!");
        }
        return new File(directory, Long.toString(fileInfo.firstKey));
    }

    private File toTempFile(FileNode fileInfo) {
        if (directory == null) {
            throw new RuntimeException("Directory is null, probably the data interface was closed already!");
        }
        return new File(directory, "tmp." + Long.toString(fileInfo.firstKey));
    }

    private Map<Long, T> readMap(FileNode file) {
        List<KeyValue<T>> values = readCleanValues(file);
        Map<Long, T> result = new HashMap<>(values.size());
        for (KeyValue<T> value : values) {
            result.put(value.getKey(), value.getValue());
        }
        return result;
    }

    private List<KeyValue<T>> readCleanValues(FileNode file) {
        try {
            byte[] buffer = getReadBuffer(file, 0, file.readSize).getBuffer();
            int expectedNumberOfValues = getLowerBoundOnNumberOfValues(file.readSize);
            List<KeyValue<T>> result = new ArrayList<>(expectedNumberOfValues);
            int position = 0;
            while (position < buffer.length) {
                long key = SerializationUtils.bytesToLong(buffer, position);
                position += LONG_SIZE;
                ReadValue<T> readValue = readValue(buffer, position, true);
                position += readValue.getSize();
                result.add(new KeyValue<>(key, readValue.getValue()));
            }
            dataWasRead();
            return result;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception while reading values from file " + toFile(file).getAbsolutePath(), ex);
        }
    }

    private List<KeyValue<T>> readCleanValuesWithKeyFilter(FileNode file, KeyFilter keyFilter) {
        try {
            byte[] buffer = getReadBuffer(file, 0, file.readSize).getBuffer();
            int expectedNumberOfValues = getLowerBoundOnNumberOfValues(file.readSize);
            List<KeyValue<T>> result = new ArrayList<>(expectedNumberOfValues);
            int position = 0;
            while (position < buffer.length) {
                long key = SerializationUtils.bytesToLong(buffer, position);
                position += LONG_SIZE;
                boolean readActualValue = keyFilter.acceptKey(key);
                ReadValue<T> readValue = readValue(buffer, position, readActualValue);
                position += readValue.getSize();
                if (readActualValue) {
                    result.add(new KeyValue<>(key, readValue.getValue()));
                }
            }
            dataWasRead();
            return result;
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected exception while reading values from file " + toFile(file).getAbsolutePath(), ex);
        }
    }

    private List<KeyValue<T>> readAllValues(FileNode file) {
        try {
            byte[] buffer = readCompleteFile(file);
            if (buffer.length > 0) {
                int expectedNumberOfValues = getLowerBoundOnNumberOfValues(file.writeSize);
                List<KeyValue<T>> result = new ArrayList<>(expectedNumberOfValues);
                //read values in buckets
                int numberOfBuckets = Math.max(1, expectedNumberOfValues / 1000);
                List[] buckets = new List[numberOfBuckets];
                for (int i = 0; i < buckets.length; i++) {
                    buckets[i] = new ArrayList(expectedNumberOfValues / numberOfBuckets);
                }
                long start = file.firstKey;
                long density = (1l << BITS_TO_DISCARD_FOR_FILE_BUCKETS) / numberOfBuckets;
                int position = 0;
                while (position < buffer.length) {
                    long key = SerializationUtils.bytesToLong(buffer, position);
                    position += LONG_SIZE;
                    ReadValue<T> readValue = readValue(buffer, position, true);
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

    private byte[] readCompleteFile(FileNode file) throws IOException {
        FileInputStream fis = new FileInputStream(toFile(file));
        byte[] buffer = new byte[file.writeSize];
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

    private List<Long> readKeys(FileNode file) throws IOException {
        List<Long> result = new ArrayList<>();
        byte[] buffer = getReadBuffer(file, 0, file.readSize).getBuffer();
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

        private FileNode previousNode;

        public FileNode lockCurrentBucketAndGetNextFile() {
            FileNode nextNode;
            if (previousNode == null) {
                nextNode = fileNodesRoot.getNode(Long.MIN_VALUE, LOCK_READ);
            } else {
                nextNode = fileNodesRoot.getNode(previousNode.lastKey, LOCK_READ);
            }
            if (nextNode == previousNode) {
                return null;
            } else {
                previousNode = nextNode;
                return nextNode;
            }
        }

    }

    public static class MetaFile {
        private FileNode fileNodesRoot;
        private long lastWrite;
        private long lastRead;

        public MetaFile(FileNode fileNodesRoot, long lastWrite, long lastRead) {
            this.fileNodesRoot = fileNodesRoot;
            this.lastRead = lastRead;
            this.lastWrite = lastWrite;
        }

        //Constructor used in serialization
        public MetaFile() {
        }

        public FileNode getFileNodesRoot() {
            return fileNodesRoot;
        }

        public void setFileNodesRoot(FileNode fileNodesRoot) {
            this.fileNodesRoot = fileNodesRoot;
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
