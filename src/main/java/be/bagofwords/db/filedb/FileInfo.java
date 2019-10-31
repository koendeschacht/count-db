package be.bagofwords.db.filedb;

import be.bagofwords.logging.Log;
import be.bagofwords.util.Pair;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

class FileInfo implements Comparable {

    private int bucketIndex;
    private long firstKey;
    private long lastKey;
    private int readSize;
    private int writeSize;
    private byte[] cachedFileContents;
    //This field is only filled in when the file is clean (i.e. not isDirty)
    private long[] fileLocationsKeys;
    private int[] fileLocationsValues;

    public FileInfo(@JsonProperty("bucketIndex") int bucketIndex,
                    @JsonProperty("firstKey") long firstKey,
                    @JsonProperty("lastKey") long lastKey,
                    @JsonProperty("readSize") int readSize,
                    @JsonProperty("writeSize") int writeSize) {
        this.bucketIndex = bucketIndex;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.readSize = readSize;
        this.writeSize = writeSize;
        if (readSize == 0) {
            fileLocationsKeys = new long[0];
            fileLocationsValues = new int[0];
        }
    }

    public long getFirstKey() {
        return firstKey;
    }

    public int getBucketIndex() {
        return bucketIndex;
    }

    public long getLastKey() {
        return lastKey;
    }

    public int getReadSize() {
        return readSize;
    }

    public void fileWasRewritten(List<Pair<Long, Integer>> fileLocations, int newReadSize, int newWriteSize) {
        fileLocations = sample(fileLocations, 50);
        this.readSize = newReadSize;
        this.writeSize = newWriteSize;
        this.fileLocationsKeys = new long[fileLocations.size()];
        this.fileLocationsValues = new int[fileLocations.size()];
        for (int i = 0; i < fileLocations.size(); i++) {
            fileLocationsKeys[i] = fileLocations.get(i).getFirst();
            fileLocationsValues[i] = fileLocations.get(i).getSecond();
            if (fileLocationsValues[i] < 0) {
                throw new RuntimeException("Illegal value for file location " + fileLocationsValues[i]);
            }
        }
    }

    @Override
    public int compareTo(Object o) {
        if (o instanceof FileInfo) {
            return Long.compare(getFirstKey(), ((FileInfo) o).getFirstKey());
        } else if (o instanceof Long) {
            return Long.compare(getFirstKey(), (Long) o);
        } else {
            throw new RuntimeException("Can not compare FileInfo with " + o);
        }
    }

    public String toString() {
        return super.toString() + " " + getFirstKey() + " " + getLastKey() + " " + getReadSize() + " " + getWriteSize();
    }

    public void increaseWriteSize(int diff) {
        this.writeSize += diff;
    }

    public long[] getFileLocationsKeys() {
        return fileLocationsKeys;
    }

    public int[] getFileLocationsValues() {
        return fileLocationsValues;
    }

    public long discardFileContents() {
        int bytesReleased = 0;
        if (cachedFileContents != null) {
            bytesReleased = cachedFileContents.length;
            cachedFileContents = null;
        }
        return bytesReleased;
    }

    @JsonIgnore
    public byte[] getCachedFileContents() {
        return cachedFileContents;
    }

    @JsonIgnore
    public void setCachedFileContents(byte[] cachedFileContents) {
        this.cachedFileContents = cachedFileContents;
    }

    public int getWriteSize() {
        return writeSize;
    }

    @JsonIgnore
    public boolean isClean() {
        return readSize == writeSize;
    }

    public void setLastKey(long lastKey) {
        this.lastKey = lastKey;
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

}
