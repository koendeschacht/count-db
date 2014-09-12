package be.bow.db.filedb4;

import be.bow.util.Pair;
import org.apache.commons.io.IOUtils;

import java.io.DataInputStream;
import java.util.List;

class FileInfo implements Comparable {

    private final long firstKey;
    private long size;
    private DataInputStream inputStream;
    private boolean isDirty;
    //This field is only filled in when the file is clean (i.e. not isDirty)
    private List<Pair<Long, Integer>> fileLocations;

    public FileInfo(long firstKey, long size) {
        this.firstKey = firstKey;
        this.size = size;
        this.isDirty = size > 0;
    }

    public DataInputStream getInputStream() {
        return inputStream;
    }

    public long getFirstKey() {
        return firstKey;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public boolean isDirty() {
        return isDirty;
    }

    public void markFileAsDirty() {
        this.isDirty = true;
    }

    public void fileIsCleaned(List<Pair<Long, Integer>> fileLocations) {
        this.fileLocations = fileLocations;
        this.isDirty = false;
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
        return getFirstKey() + " " + getSize() + " " + isDirty();
    }

    public void increaseSize(long diff) {
        this.size += diff;
    }

    public List<Pair<Long, Integer>> getFileLocations() {
        return fileLocations;
    }

    public void setInputStream(DataInputStream inputStream) {
        this.inputStream = inputStream;
    }

    public boolean discardInputStream() {
        if (inputStream != null) {
            IOUtils.closeQuietly(inputStream);
            inputStream = null;
            return true;
        } else {
            return false;
        }
    }

}
