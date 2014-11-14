package be.bagofwords.db.bloomfilter;

import be.bagofwords.util.ByteArraySerializable;

import java.io.*;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/3/14.
 */
public class LongBloomFilterWithCheckSum extends LongBloomFilter implements ByteArraySerializable {


    private long dataCheckSum;

    public LongBloomFilterWithCheckSum(long expectedSize, double fpp) {
        super(expectedSize, fpp);
    }

    public LongBloomFilterWithCheckSum(BitArray bitArray, int numOfHashFunctions) {
        super(bitArray, numOfHashFunctions);
    }

    public LongBloomFilterWithCheckSum(byte[] array) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(array);
            DataInputStream dis = new DataInputStream(bis);
            dataCheckSum = dis.readLong();
            numOfHashFunctions = dis.readInt();
            int lengthOfData = dis.readInt();
            long[] data = new long[lengthOfData];
            for (int i = 0; i < data.length; i++) {
                data[i] = dis.readLong();
            }
            bits = new BitArray(data);
            dis.close();
        } catch (IOException exp) {
            throw new RuntimeException("Failed to deserialize bloom filter", exp);
        }
    }

    public long getDataCheckSum() {
        return dataCheckSum;
    }

    public void setDataCheckSum(long dataCheckSum) {
        this.dataCheckSum = dataCheckSum;
    }

    public void increaseDataCheckSum() {
        this.dataCheckSum++;
    }

    //Used for serialization

    public LongBloomFilterWithCheckSum() {
    }

    @Override
    public byte[] toByteArray() {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            dos.writeLong(dataCheckSum);
            dos.writeInt(getNumOfHashFunctions());
            long[] data = getBits().getData();
            dos.writeInt(data.length);
            for (long value : data) {
                dos.writeLong(value);
            }
            dos.close();
            return bos.toByteArray();
        } catch (IOException exp) {
            throw new RuntimeException("Failed to serialize bloom filter", exp);
        }
    }
}
