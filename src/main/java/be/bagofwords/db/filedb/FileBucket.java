package be.bagofwords.db.filedb;

import be.bagofwords.ui.UI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;

public class FileBucket implements Comparable<FileBucket> {

    private static final int NUMBER_OF_READ_PERMITS = 1000;

    private final long firstKey; //inclusive
    private final long lastKey; //inclusive
    private List<FileInfo> files;
    private final Semaphore appendLock;
    private final Semaphore rewriteLock;

    public FileBucket(long firstKey, long lastKey) {
        this.firstKey = firstKey;
        this.lastKey = lastKey;
        this.files = new ArrayList<>();
        this.appendLock = new Semaphore(1);
        this.rewriteLock = new Semaphore(NUMBER_OF_READ_PERMITS);
    }

    public List<FileInfo> getFiles() {
        return files;
    }

    public int getFileInd(long key) {
        if (files.size() < 10) {
            for (int i = 0; i < files.size(); i++) {
                if (files.get(i).getFirstKey() > key) {
                    if (i == 0) {
                        throw new RuntimeException("Incorrect bucket starting from " + getFirstKey() + " for key " + key);
                    } else {
                        return i - 1;
                    }
                }
            }
            return files.size() - 1;
        } else {
            int pos = Collections.binarySearch((List) files, key);
            if (pos < 0) {
                pos = -(pos + 2);
            }
            if (pos == -1) {
                UI.write("ARRAY INDEX OUT OF BOUNDS!!!!");
                UI.write("Was looking for key " + key);
                UI.write("In bucket starting with key " + getFirstKey());
                UI.write("In files ");
                for (FileInfo file : files) {
                    UI.write("   " + file.getFirstKey());
                }
            }
            return pos;
        }
    }

    public long getFirstKey() {
        return firstKey;
    }

    public FileInfo getFile(long key) {
        return files.get(getFileInd(key));
    }

    public long getLastKey() {
        return lastKey;
    }

    public void lockAppend() {
        appendLock.acquireUninterruptibly();
    }

    public void unlockAppend() {
        appendLock.release();
        if (appendLock.availablePermits() > 1) {
            throw new RuntimeException("Illegal state of append lock: too many unlocks!");
        }
    }

    public void lockRead() {
        rewriteLock.acquireUninterruptibly(1);
    }

    public void unlockRead() {
        rewriteLock.release(1);
    }

    public void lockRewrite() {
        rewriteLock.acquireUninterruptibly(NUMBER_OF_READ_PERMITS);
    }

    public void unlockRewrite() {
        rewriteLock.release(NUMBER_OF_READ_PERMITS);
        if (rewriteLock.availablePermits() > NUMBER_OF_READ_PERMITS) {
            throw new RuntimeException("Illegal state of rewrite lock: too many unlocks!");
        }
    }

    public String toString() {
        return super.toString() + " " + firstKey + ", appendLock=" + appendLock.availablePermits() + " rewriteLock=" + rewriteLock.availablePermits();
    }

    @Override
    public int compareTo(FileBucket o) {
        return Long.compare(firstKey, o.getFirstKey());
    }

    public void setFiles(List<FileInfo> files) {
        this.files = files;
    }
}
