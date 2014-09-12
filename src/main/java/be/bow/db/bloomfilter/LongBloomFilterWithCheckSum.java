package be.bow.db.bloomfilter;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/3/14.
 */
public class LongBloomFilterWithCheckSum extends LongBloomFilter {


    private long dataCheckSum;

    public LongBloomFilterWithCheckSum(long expectedSize, double fpp) {
        super(expectedSize, fpp);
    }

    public LongBloomFilterWithCheckSum(BitArray bitArray, int numOfHashFunctions) {
        super(bitArray, numOfHashFunctions);
    }

    public long getDataCheckSum() {
        return dataCheckSum;
    }

    public void setDataCheckSum(long dataCheckSum) {
        this.dataCheckSum = dataCheckSum;
    }

    //Used for serialization

    public LongBloomFilterWithCheckSum() {
    }
}
