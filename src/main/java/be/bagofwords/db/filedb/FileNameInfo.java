package be.bagofwords.db.filedb;

public class FileNameInfo {

    public int bucketInd;
    public long firstKey;

    public FileNameInfo(int bucketInd, long firstKey) {
        this.bucketInd = bucketInd;
        this.firstKey = firstKey;
    }
}
