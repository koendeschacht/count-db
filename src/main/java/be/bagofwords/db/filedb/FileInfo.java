package be.bagofwords.db.filedb;

import be.bagofwords.util.Pair;

import java.util.List;

class FileInfo implements Comparable {

    private final long firstKey;
    private int readSize;
    private int writeSize;
    private byte[] cachedFileContents;
    //This field is only filled in when the file is clean (i.e. not isDirty)
    private long[] fileLocationsKeys;
    private int[] fileLocationsValues;

    public FileInfo(long firstKey, int readSize, int writeSize) {
        this.firstKey = firstKey;
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

    public int getReadSize() {
        return readSize;
    }

    public void fileWasRewritten(List<Pair<Long, Integer>> fileLocations, int newReadSize, int newWriteSize) {
        this.readSize = newReadSize;
        this.writeSize = newWriteSize;
        this.fileLocationsKeys = new long[fileLocations.size()];
        this.fileLocationsValues = new int[fileLocations.size()];
        for (int i = 0; i < fileLocations.size(); i++) {
            fileLocationsKeys[i] = fileLocations.get(i).getFirst();
            fileLocationsValues[i] = fileLocations.get(i).getSecond();
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
        return super.toString() + " " + getFirstKey() + " " + getReadSize() + " " + getWriteSize();
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
