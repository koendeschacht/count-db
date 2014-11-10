package be.bagofwords.db.filedb;

import be.bagofwords.util.Pair;

import java.util.Collections;
import java.util.List;

class FileInfo implements Comparable {

    private final long firstKey;
    private int readSize;
    private int writeSize;
    private byte[] cachedFileContents;
    //This field is only filled in when the file is clean (i.e. not isDirty)
    private List<Pair<Long, Integer>> fileLocations;

    public FileInfo(long firstKey, int readSize, int writeSize) {
        this.firstKey = firstKey;
        this.readSize = readSize;
        if (readSize == 0) {
            fileLocations = Collections.emptyList();
        }
    }

    public long getFirstKey() {
        return firstKey;
    }

    public int getReadSize() {
        return readSize;
    }

    public void fileWasRewritten(List<Pair<Long, Integer>> fileLocations, int newReadSize, int newWriteSize) {
        this.readSize = newReadSize;
        this.writeSize = newWriteSize;
        this.fileLocations = fileLocations;
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
        return super.toString() + " " + getFirstKey() + " " + getReadSize() + " " + getWriteSize();
    }

    public void increaseWriteSize(int diff) {
        this.writeSize += diff;
    }

    public List<Pair<Long, Integer>> getFileLocations() {
        return fileLocations;
    }

    public long discardFileContents() {
        int bytesReleased = 0;
        if (cachedFileContents != null) {
            bytesReleased = cachedFileContents.length;
            cachedFileContents = null;
        }
        return bytesReleased;
    }

    public byte[] getCachedFileContents() {
        return cachedFileContents;
    }

    public void setCachedFileContents(byte[] cachedFileContents) {
        this.cachedFileContents = cachedFileContents;
    }

    public int getWriteSize() {
        return writeSize;
    }

    public boolean isDirty() {
        return readSize < writeSize;
    }
}
