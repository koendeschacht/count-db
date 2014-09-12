package be.bow.db.filedb4;

import be.bow.application.file.FilesCollection;
import be.bow.application.file.OpenFilesManager;
import be.bow.application.memory.MemoryGobbler;
import be.bow.iterator.CloseableIterator;
import be.bow.iterator.IterableUtils;
import be.bow.iterator.SimpleIterator;
import be.bow.ui.UI;
import be.bow.util.*;
import be.bow.db.Combinator;
import be.bow.db.CoreDataInterface;
import be.bow.db.DataInterface;

import java.io.*;
import java.util.*;

public class FileDataInterface<T extends Object> extends CoreDataInterface<T> implements FilesCollection, MemoryGobbler {

    private static final long MAX_FILE_SIZE_WRITE = 10 * 1024 * 1024;
    private static final long MAX_FILE_SIZE_READ = 1024 * 1024;
    private static final long BITS_TO_DISCARD_FOR_FILE_BUCKETS = 58;
    private static final int BATCH_SIZE = 1000000;
    private static final long LONG_NULL = Long.MAX_VALUE;
    private static final double DOUBLE_NULL = Double.MAX_VALUE;
    private static final String CLEAN_FILES_FILE = "CLEAN_FILES";

    private final OpenFilesManager openFilesManager;

    private File directory;
    private Map<Long, FileBucket> fileBuckets;
    private ReduceFileSizeThread reduceFileSizeThread;
    private long timeOfLastWrite = 0;
    private long timeOfLastRead = 0;
    private long timeOfLastWriteOfCleanFileList = 0;

    //DEBUG:
    public int numOfCleanReads = 0;
    public int numOfDirtyReads = 0;
    long timeReading = 0;
    long timeSorting = 0;
    long timeWriting = 0;
    long totalTime = 0;


    public FileDataInterface(OpenFilesManager openFilesManager, Combinator<T> combinator, Class<T> objectClass, String directory, String nameOfSubset) {
        super(nameOfSubset, objectClass, combinator);
        this.openFilesManager = openFilesManager;
        this.directory = new File(directory, nameOfSubset);
        initializeFileBuckets();
        checkDataDir();
        startThreads();
        timeOfLastWrite = System.currentTimeMillis();
        timeOfLastRead = System.currentTimeMillis();
    }

    @Override
    public T readInt(long keyToRead) {
        timeOfLastRead = System.currentTimeMillis();
        FileBucket bucket = getBucket(keyToRead);
        bucket.lock();
        FileInfo file = bucket.getFile(keyToRead);
        try {
            if (file.getSize() == 0) {
                return null;
            }
            if (!file.isDirty()) {
                return readClean(keyToRead, file);
            } else {
                return readDirty(keyToRead, file);
            }
        } catch (Exception exp) {
            throw new RuntimeException("Error in file " + toFile(file).getAbsolutePath(), exp);
        } finally {
            bucket.unlock();
        }
    }

    @Override
    public void writeInt0(long key, T value) {
        timeOfLastWrite = System.currentTimeMillis();
        FileBucket bucket = getBucket(key);
        bucket.lock();
        FileInfo fileInfo = bucket.getFile(key);
        try {
            DataOutputStream dos = getOutputStream(fileInfo, true);
            long extraSize = writeValue(dos, key, value, getObjectClass());
            fileInfo.increaseSize(extraSize);
            fileInfo.markFileAsDirty();
            dos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            bucket.unlock();
        }
    }

    @Override
    public void writeInt0(Iterator<KeyValue<T>> entries) {
        long start = System.currentTimeMillis();
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
                bucket.lock();
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
                            file.increaseSize(extraSize);
                        }
                        file.markFileAsDirty();
                        dos.close();
                        timeOfLastWrite = System.currentTimeMillis();
                    }
                } catch (IOException exp) {
                    throw new RuntimeException(exp);
                } finally {
                    bucket.unlock();
                }
            }
        }
        totalTimeWrite += System.currentTimeMillis() - start;
    }

    /**
     * This method will only work performantly if the keys are read in an ordered fashion
     */

    @Override
    public CloseableIterator<KeyValue<T>> read(final Iterator<Long> keyIterator) {
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private FileInfo currentFile;
            private Map<Long, T> valuesInFile;

            @Override
            public synchronized KeyValue<T> next() throws Exception {
                while (keyIterator.hasNext()) {
                    long key = keyIterator.next();
                    FileBucket currentBucket = getBucket(key);
                    FileInfo file = currentBucket.getFile(key);
                    if (file != currentFile) {
                        currentFile = file;
                        currentBucket.lock();
                        valuesInFile = readMap(currentFile, false);
                        currentBucket.unlock();
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
        final CloseableIterator<Pair<FileInfo, FileBucket>> fileIterator = createFileIterator();
        return IterableUtils.iterator(new SimpleIterator<KeyValue<T>>() {

            private Iterator<Pair<Long, T>> valuesInFileIt;

            @Override
            public KeyValue<T> next() throws Exception {
                while ((valuesInFileIt == null || !valuesInFileIt.hasNext()) && fileIterator.hasNext()) {
                    Pair<FileInfo, FileBucket> next = fileIterator.next();
                    next.getSecond().lock();
                    List<Pair<Long, T>> sortedEntries = readValues(next.getFirst(), false);
                    next.getSecond().unlock();
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
        final CloseableIterator<Pair<FileInfo, FileBucket>> fileIterator = createFileIterator();
        return IterableUtils.iterator(new SimpleIterator<Long>() {

            private Iterator<Long> valuesInFileIt;

            @Override
            public Long next() throws Exception {
                while ((valuesInFileIt == null || !valuesInFileIt.hasNext()) && fileIterator.hasNext()) {
                    Pair<FileInfo, FileBucket> next = fileIterator.next();
                    next.getSecond().lock();
                    List<Long> sortedEntries = readKeys(next.getFirst());
                    next.getSecond().unlock();
                    valuesInFileIt = sortedEntries.iterator();
                }
                if (valuesInFileIt != null && valuesInFileIt.hasNext()) {
                    return valuesInFileIt.next();
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
    public void closeOpenFiles(double ratio) {
        Random random = new Random();
        for (FileBucket bucket : fileBuckets.values()) {
            if (bucket.tryLock()) {
                for (FileInfo fileInfo : bucket.getFiles()) {
                    if (random.nextDouble() < ratio && fileInfo.discardInputStream()) {
                        openFilesManager.registerClosedFile();
                    }
                }
                bucket.unlock();
            }
        }
    }

    @Override
    public void freeMemory() {
        closeOpenFiles(1.0);
    }

    @Override
    public long apprSize() {
        int numOfFilesToSample = fileBuckets.size() / 10;
        long numOfObjects = 0;
        long sizeOfSampledFiles = 0;
        int numOfSampledFiles = 0;
        long sizeOfAllFiles = 0;
        try {
            CloseableIterator<Pair<FileInfo, FileBucket>> fileIt = createFileIterator();
            while (fileIt.hasNext()) {
                Pair<FileInfo, FileBucket> curr = fileIt.next();
                long fileSize = curr.getFirst().getSize();
                if (numOfSampledFiles < numOfFilesToSample) {
                    curr.getSecond().lock();
                    List<Long> keys = readKeys(curr.getFirst());
                    curr.getSecond().unlock();
                    numOfObjects += keys.size();
                    sizeOfSampledFiles += fileSize;
                    if (fileSize == 0 && !keys.isEmpty()) {
                        UI.writeError("Something is wrong with file " + curr.getFirst().getFirstKey());
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
    public DataInterface getImplementingDataInterface() {
        return null;
    }

    @Override
    public void flush() {
        //Do nothing, always flushed...
    }

    @Override
    public synchronized void close() {
        for (FileBucket bucket : fileBuckets.values()) {
            bucket.lock();
            for (FileInfo file : bucket.getFiles()) {
                if (file.discardInputStream()) {
                    openFilesManager.registerClosedFile();
                }
            }
            bucket.unlock();
        }
        terminateThreads();
    }

    @Override
    public synchronized void dropAllData() {
        try {
            terminateThreads();
            for (FileBucket bucket : fileBuckets.values()) {
                bucket.lock();
                for (FileInfo file : bucket.getFiles()) {
                    deleteFile(file);
                }
                bucket.getFiles().clear();
                bucket.unlock();
            }
            initializeFiles();
            startThreads();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void startThreads() {
        reduceFileSizeThread = new ReduceFileSizeThread();
        reduceFileSizeThread.start();
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
        } else {
            byte[] objAsBytes = SerializationUtils.objectToCompressedBytes(value);
            dos.writeInt(objAsBytes.length);
            dos.write(objAsBytes);
            return 8 + 4 + objAsBytes.length;
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
        } else {
            int length = dis.readInt();
            byte[] objectAsBytes = new byte[length];
            int bytesRead = dis.read(objectAsBytes);
            if (bytesRead < objectAsBytes.length) {
                throw new RuntimeException("Could not read complete object, expected " + length + " bytes, received " + bytesRead);
            }
            return SerializationUtils.compressedBytesToObject(objectAsBytes, objectClass);
        }
    }

    private void initializeFileBuckets() {
        fileBuckets = new HashMap<>();
        long start = Long.MIN_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        long end = Long.MAX_VALUE >> BITS_TO_DISCARD_FOR_FILE_BUCKETS;
        for (long val = start; val <= end; val++) {
            long firstKey = val << BITS_TO_DISCARD_FOR_FILE_BUCKETS;
            long lastKey = ((val + 1) << BITS_TO_DISCARD_FOR_FILE_BUCKETS) - 1;
            if (lastKey < firstKey) {
                //overflow
                lastKey = Long.MAX_VALUE;
            }
            fileBuckets.put(val, new FileBucket(firstKey, lastKey));
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
                long size = new File(directory, file).length();
                FileInfo fileInfo = new FileInfo(key, size);
                boolean isClean = cleanFileInfo.containsKey(key);
                if (isClean) {
                    fileInfo.fileIsCleaned(cleanFileInfo.get(key));
                }
                bucket.getFiles().add(fileInfo);
            }
        }
        for (FileBucket bucket : fileBuckets.values()) {
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
                    throw new RuntimeException("Missing file in " + getName() + " ? Expected file " + bucket.getFirstKey());
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

    private void writeCleanFilesList() {
        try {
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(new File(directory, CLEAN_FILES_FILE)));
            for (FileBucket bucket : fileBuckets.values()) {
                bucket.lock(); //Make sure no other thread can write to this file bucket
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
                bucket.unlock();
            }
            dos.close();
        } catch (IOException exp) {
            throw new RuntimeException("Received exception while writing list of clean files", exp);
        }
    }

    private FileBucket getBucket(long key) {
        return fileBuckets.get(key >> BITS_TO_DISCARD_FOR_FILE_BUCKETS);
    }

    private T readDirty(long keyToRead, FileInfo file) throws IOException {
        DataInputStream dis;
        numOfDirtyReads++;
        dis = getInputStream(file);
        T currVal = null;
        boolean finished = false;
        byte keyBuffer[] = new byte[8];
        byte firstByte = (byte) (keyToRead >> 56);
        while (!finished) {
            int bytesRead = dis.read(keyBuffer);
            if (bytesRead <= 0) {
                finished = true;
            }
            long key = -1;
            if (keyBuffer[0] == firstByte) {
                key = toLong(keyBuffer);
            }
            if (!finished) {
                if (key == keyToRead) {
                    T value = readValue(getObjectClass(), dis);
                    if (currVal == null || value == null) {
                        currVal = value;
                    } else {
                        currVal = getCombinator().combine(currVal, value);
                    }
                } else {
                    skipValue(dis);
                }
            }
        }
        return currVal;
    }

    private T readClean(long keyToRead, FileInfo file) throws IOException {
        DataInputStream dis;
        numOfCleanReads++;
        //See whether we can skip a portion of the file?
        int pos = Collections.binarySearch((List) file.getFileLocations(), keyToRead);
        if (pos == -1) {
            //Before first key, value can not be in this file
            return null;
        }
        if (pos < 0) {
            pos = -(pos + 1);
        }
        if (pos == file.getFileLocations().size() || file.getFileLocations().get(pos).getFirst() > keyToRead) {
            pos--;
        }
        if (file.getFileLocations().get(pos).getFirst() > keyToRead) {
            throw new RuntimeException("Critical, unexpected state while reading value");
        }
        int startPos = file.getFileLocations().get(pos).getSecond();
        dis = getInputStream(file);
        dis.skipBytes(startPos);
        byte keyBuffer[] = new byte[8];
        byte firstByte = (byte) (keyToRead >> 56);
        while (true) {
            int bytesRead = dis.read(keyBuffer);
            if (bytesRead <= 0) {
                return null; //end of file
            }
            if (keyBuffer[0] < firstByte) {
                //key too small
                skipValue(dis);
            } else if (keyBuffer[0] == firstByte) {
                long key = toLong(keyBuffer);
                if (key == keyToRead) {
                    return readValue(getObjectClass(), dis);
                } else if (key > keyToRead) {
                    return null;
                } else {
                    skipValue(dis);
                }
            } else if (keyBuffer[0] > firstByte) {
                //key too large
                return null;
            }
        }
    }

    private DataInputStream getInputStream(FileInfo file) throws IOException {
        DataInputStream inputStream = file.getInputStream();
        if (inputStream != null) {
            inputStream.reset();
        } else {
            openFilesManager.registerOpenFile();
            inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(toFile(file))));
            if (file.getSize() > Integer.MAX_VALUE) {
                throw new RuntimeException("Can not create input stream for file " + file + ", size=" + file.getSize());
            }
            inputStream.mark((int) file.getSize() + 1024);
            file.setInputStream(inputStream);
        }
        return inputStream;
    }

    private long toLong(byte[] buffer) {
        return (((long) buffer[0] << 56) +
                ((long) (buffer[1] & 255) << 48) +
                ((long) (buffer[2] & 255) << 40) +
                ((long) (buffer[3] & 255) << 32) +
                ((long) (buffer[4] & 255) << 24) +
                ((buffer[5] & 255) << 16) +
                ((buffer[6] & 255) << 8) +
                ((buffer[7] & 255) << 0));
    }

    private void skipValue(DataInputStream dis) throws IOException {
        //Skip some bytes
        Class<T> objectClass = getObjectClass();
        if (objectClass == Long.class) {
            dis.skipBytes(8);
        } else if (objectClass == Double.class) {
            dis.skipBytes(8);
        } else {
            int length = dis.readInt();
            dis.skipBytes(length);
        }
    }

    private DataOutputStream getOutputStream(FileInfo fileInfo, boolean append) throws FileNotFoundException {
        if (fileInfo.discardInputStream()) {
            openFilesManager.registerClosedFile();
        }
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(toFile(fileInfo), append)));
    }

    private File toFile(FileInfo fileInfo) {
        if (directory == null) {
            throw new RuntimeException("Directory is null, probably the datainterface was closed already!");
        }
        return new File(directory, Long.toString(fileInfo.getFirstKey()));
    }

    /**
     * This method assumes that the file bucket for the given file is already locked
     */

    private Map<Long, T> readMap(FileInfo file, boolean isReduceFileThread) {
        List<Pair<Long, T>> values = readValues(file, isReduceFileThread);
        Map<Long, T> result = new HashMap<>(values.size());
        for (Pair<Long, T> value : values) {
            result.put(value.getFirst(), value.getSecond());
        }
        return result;
    }

    /**
     * This method assumes that the file bucket for the given file is already locked
     */

    private List<Pair<Long, T>> readValues(FileInfo file, boolean isReduceFileThread) {
        if (!isReduceFileThread) {
            timeOfLastRead = System.currentTimeMillis();
        }
        try {
            if (file.getSize() > 0) {
                DataInputStream dis = getInputStream(file);
                int expectedNumberOfLongValues = (int) (file.getSize() / (16));
                if (!file.isDirty()) {
                    List<Pair<Long, T>> result = new ArrayList<>(expectedNumberOfLongValues);
                    if (isReduceFileThread) {
                        timeReading -= System.nanoTime();
                    }
                    boolean finished = false;
                    while (!finished) {
                        long key = -1;
                        try {
                            key = dis.readLong();
                        } catch (EOFException exp) {
                            finished = true;
                        }
                        if (!finished) {
                            T value = readValue(getObjectClass(), dis);
                            result.add(new Pair<>(key, value));
                        }
                    }
                    if (isReduceFileThread) {
                        timeReading += System.nanoTime();
                    }
                    return result;
                } else {
                    //Read values in buckets
                    int numberOfBuckets = getObjectClass() == Long.class ? Math.max(1, (int) (file.getSize() / (16 * 1000))) : 100;
                    List[] buckets = new List[numberOfBuckets];
                    for (int i = 0; i < buckets.length; i++) {
                        buckets[i] = new ArrayList(expectedNumberOfLongValues / numberOfBuckets);
                    }
                    boolean finished = false;
                    long start = file.getFirstKey();
                    long density = (1l << BITS_TO_DISCARD_FOR_FILE_BUCKETS) / numberOfBuckets;
                    if (isReduceFileThread) {
                        timeReading -= System.nanoTime();
                    }
                    while (!finished) {
                        long key = -1;
                        try {
                            key = dis.readLong();
                        } catch (EOFException exp) {
                            finished = true;
                        }
                        if (!finished) {
                            T value = readValue(getObjectClass(), dis);
                            int bucketInd = (int) ((key - start) / density);
                            if (bucketInd == buckets.length) {
                                bucketInd--; //rounding error?
                            }
                            buckets[bucketInd].add(new Pair<>(key, value));
                        }
                    }
                    if (isReduceFileThread) {
                        timeReading += System.nanoTime();
                    }
                    if (isReduceFileThread) {
                        timeSorting -= System.nanoTime();
                    }
                    List<Pair<Long, T>> combinedResult = new ArrayList<>();
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
                                    if (currVal == null) {
                                        if (nextVal != null) {
                                            //First value was a delete, keep only second value
                                            combinedVal = nextVal;
                                        } else {
                                            combinedVal = null;
                                        }
                                    } else if (nextVal == null) {
                                        //Second value was a delete, forget first value
                                        combinedVal = null;
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
                                combinedResult.add(currPair);
                            }
                        }
                        buckets[bucketInd] = null; //Free some memory
                    }
                    if (isReduceFileThread) {
                        timeSorting += System.nanoTime();
                    }
                    return combinedResult;
                }
            } else {
                return Collections.emptyList();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException("In interface " + getName() + ", tried to read file " + toFile(file).getAbsolutePath() + ", size should be " + file.getSize(), e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableIterator<Pair<FileInfo, FileBucket>> createFileIterator() {
        final Iterator<FileBucket> bucketIterator = fileBuckets.values().iterator();
        return new CloseableIterator<Pair<FileInfo, FileBucket>>() {

            private int fileInd = -1;
            private FileBucket currBucket;

            @Override
            protected void closeInt() {
                //OK
            }

            @Override
            public boolean hasNext() {
                return bucketIterator.hasNext() || (currBucket != null && fileInd < currBucket.getFiles().size() - 1);
            }

            @Override
            public Pair<FileInfo, FileBucket> next() {
                fileInd++;
                while ((currBucket == null || fileInd >= currBucket.getFiles().size()) && bucketIterator.hasNext()) {
                    currBucket = bucketIterator.next();
                    fileInd = 0;
                }
                return new Pair<>(currBucket.getFiles().get(fileInd), currBucket);
            }
        };
    }

    private List<Long> readKeys(FileInfo file) throws IOException {
        if (file.isDirty()) {
            List<Pair<Long, T>> values = readValues(file, false);
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
        timeOfLastRead = System.currentTimeMillis();
        List<Long> result = new ArrayList<>();
        if (file.getSize() == 0) {
            return result;
        }
        DataInputStream dis = getInputStream(file);
        boolean finished = false;
        while (!finished) {
            try {
                result.add(dis.readLong());
                //Skip some bytes
                if (getObjectClass() == Long.class) {
                    dis.skipBytes(8);
                } else if (getObjectClass() == Double.class) {
                    dis.skipBytes(8);
                } else {
                    int length = dis.readInt();
                    dis.skipBytes(length);
                }
            } catch (EOFException ex) {
                finished = true;
            }
        }
        return result;
    }

    private void deleteFile(FileInfo file) {
        boolean success = toFile(file).delete();
        if (!success) {
            throw new RuntimeException("Failed to delete file " + toFile(file).getAbsolutePath());
        }
        if (file.discardInputStream()) {
            openFilesManager.registerClosedFile();
        }
    }

    private void terminateThreads() {
        reduceFileSizeThread.close();
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

    public class ReduceFileSizeThread extends SafeThread {

        public ReduceFileSizeThread() {
            super("ReduceFileSizeThread_" + FileDataInterface.this.getName(), false);
        }

        public void runInt() {
            while (!isTerminateRequested()) {
                long start = System.currentTimeMillis();
                totalTime -= System.nanoTime();
                int numOfFilesRewritten = 0;
                int numOfNewFiles = 0;
                int numOfFilesMerged = 0;
                boolean someFilesAreDirty = false;
                List<FileBucket> buckets = new ArrayList<>(fileBuckets.values());
                for (int j = 0; j < buckets.size() && !isTerminateRequested(); j++) {
                    FileBucket bucket = buckets.get(j);
                    boolean bucketLocked = false;
                    try {
                        for (int i = 0; i < bucket.getFiles().size(); i++) {
                            FileInfo file = bucket.getFiles().get(i);
                            someFilesAreDirty |= file.isDirty();
                            boolean inWritePhase = timeOfLastWrite > timeOfLastRead;
                            boolean cleanFile;
                            long maxFileSize;
                            if (inWritePhase) {
                                //Optimize file size for writes (larger files, lazy cleaning)
                                cleanFile = file.isDirty() && file.getSize() > MAX_FILE_SIZE_WRITE;
                                maxFileSize = MAX_FILE_SIZE_WRITE / 4;
                            } else {
                                //Optimize file size for random reads (smaller files, eager cleaning)
                                cleanFile = file.isDirty();
                                maxFileSize = MAX_FILE_SIZE_READ;
                            }
                            if (cleanFile) {
                                if (!bucketLocked) {
                                    bucketLocked = true;
                                    bucket.lock();
                                }
                                List<Pair<Long, T>> values = readValues(file, true);
                                int filesMergedWithThisFile = inWritePhase ? 0 : mergeFileIfTooSmall(bucket, i, file, maxFileSize, values);
                                numOfFilesMerged += filesMergedWithThisFile;
                                DataOutputStream dos = getOutputStream(file, false);
                                file.setSize(0);
                                List<Pair<Long, Integer>> fileLocations = new ArrayList<>();
                                timeWriting -= System.nanoTime();
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
                                        bucket.getFiles().add(i + 1, file);
                                        i++;
                                        dos = getOutputStream(file, false);
                                        numOfNewFiles++;
                                    }
                                    fileLocations.add(new Pair<>(key, sizeToInteger(file.getSize())));
                                    dos.write(dataToWrite);
                                    file.increaseSize(dataToWrite.length);
                                }
                                file.fileIsCleaned(sample(fileLocations, 100));
                                timeWriting += System.nanoTime();
                                dos.close();
                                numOfFilesRewritten++;
                            }
                        }
                    } catch (IOException exp) {
                        UI.writeError("IOException while rewriting file", exp);
                    } finally {
                        if (bucketLocked) {
                            checkBucket(bucket);
                            bucket.unlock();
                        }
                    }
                }
                totalTime += System.nanoTime();
                if (numOfFilesRewritten > 0) {
                    UI.write("Rewrote " + numOfFilesRewritten + ",  created " + numOfNewFiles + " and merged " + numOfFilesMerged + " file(s) for DI " + FileDataInterface.this.getName() + " " + (System.currentTimeMillis() - timeOfLastWrite) + "/" + (System.currentTimeMillis() - timeOfLastRead) + " ms since last write/read, took " + (System.currentTimeMillis() - start) + " ms. " + timeReading + " " + timeSorting + " " + timeWriting + " " + totalTime);
                }
                if (numOfFilesRewritten > 0 || (someFilesAreDirty && System.currentTimeMillis() - timeOfLastWriteOfCleanFileList > 1000)) {
                    writeCleanFilesList();
                    timeOfLastWriteOfCleanFileList = System.currentTimeMillis();
                }
                Utils.threadSleep(50);
            }
        }

        private void checkBucket(FileBucket bucket) {
            if (!bucket.getFiles().isEmpty()) {
                if (bucket.getFirstKey() != bucket.getFiles().get(0).getFirstKey()) {
                    throw new RuntimeException("Missing file " + bucket.getFirstKey() + " in " + getName());
                }
            }
        }

        private int mergeFileIfTooSmall(FileBucket bucket, int currentFileInd, FileInfo file, long maxFileSize, List<Pair<Long, T>> values) {
            int nextFileInd = currentFileInd + 1;
            long combinedSize = file.getSize();
            while (nextFileInd < bucket.getFiles().size() && combinedSize + bucket.getFiles().get(nextFileInd).getSize() < maxFileSize) {
                //Combine the files
                FileInfo nextFile = bucket.getFiles().remove(nextFileInd);
                values.addAll(readValues(nextFile, true)); //Standard put of java maps since no identical keys should be present in the two files
                combinedSize += nextFile.getSize();
                deleteFile(nextFile);
                //note that nextFileInd does not need to advance, since we just removed the file at that position
            }
            return nextFileInd - currentFileInd - 1;
        }
    }

}
