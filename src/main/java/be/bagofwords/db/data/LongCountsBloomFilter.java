package be.bagofwords.db.data;

import be.bagofwords.util.HashUtils;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

import java.io.Serializable;
import java.util.Arrays;

@JsonIgnoreProperties("dataCheckSum")
public class LongCountsBloomFilter implements Serializable {

    private int numOfHashFunctions;
    private ByteArray bytes;

    public LongCountsBloomFilter(long expectedSize, double fpp) {
        if (expectedSize > Integer.MAX_VALUE) {
            throw new RuntimeException("Creating a bloomfilter currently not supported for size " + expectedSize);
        }
        int numOfBytes = optimalNumOfBytes(expectedSize, fpp);
        bytes = new ByteArray(numOfBytes);
        numOfHashFunctions = optimalNumOfHashFunctions(expectedSize, numOfBytes);
    }

    public LongCountsBloomFilter(ByteArray bytes, int numOfHashFunctions) {
        this.bytes = bytes;
        this.numOfHashFunctions = numOfHashFunctions;
    }

    public int getMaxCount(long hash64) {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        if (hash1 == 0 || hash2 == 0) {
            hash64 = HashUtils.randomDistributeHash(hash64);
            hash1 = (int) hash64;
            hash2 = (int) (hash64 >>> 32);
        }
        int min = Byte.MAX_VALUE - Byte.MIN_VALUE;
        for (int i = 1; i <= numOfHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            min = Math.min(min, bytes.get(nextHash % bytes.size()));
        }
        return min;
    }

    public synchronized <T> void addCount(long hash64, int count) {
        int currCount = getMaxCount(hash64);
        int newCount = Math.min(count + currCount, Byte.MAX_VALUE - Byte.MIN_VALUE);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        if (hash1 == 0 || hash2 == 0) {
            hash64 = HashUtils.randomDistributeHash(hash64);
            hash1 = (int) hash64;
            hash2 = (int) (hash64 >>> 32);
        }
        for (int i = 1; i <= numOfHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            bytes.set(nextHash % bytes.size(), newCount);
        }
    }

    private static int optimalNumOfBytes(long expectedSize, double fpp) {
        if (fpp == 0) {
            fpp = Double.MIN_VALUE;
        }
        double result = -expectedSize * Math.log(fpp) / (Math.log(2) * Math.log(2));
        if (result > Integer.MAX_VALUE) {
            throw new RuntimeException("Number of required bytes too large!");
        }
        return (int) result;
    }

    private static int optimalNumOfHashFunctions(long expectedSize, long numOfBytes) {
        if (expectedSize == 0) {
            expectedSize = 1;
        }
        return Math.max(1, (int) Math.round(numOfBytes / expectedSize * Math.log(2)));
    }

    //Used for serialization
    public LongCountsBloomFilter() {
    }

    public int getNumOfHashFunctions() {
        return numOfHashFunctions;
    }

    public void setNumOfHashFunctions(int numOfHashFunctions) {
        this.numOfHashFunctions = numOfHashFunctions;
    }


    public double expectedFpp() {
        return Math.pow((double) bytes.computeBitCount() / bytes.size(), numOfHashFunctions);
    }

    public LongCountsBloomFilter clone() {
        return new LongCountsBloomFilter(getBytes().clone(), numOfHashFunctions);
    }

    //Json serialization

    public ByteArray getBytes() {
        return bytes;
    }

    public void setBytes(ByteArray bytes) {
        this.bytes = bytes;
    }

    public static class LongFunnel implements Funnel<Long> {
        public void funnel(Long s, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(s);
        }
    }

    private byte max(byte val1, byte val2) {
        if (val1 > val2) {
            return val1;
        } else {
            return val2;
        }
    }

    public class ByteArray {

        private byte[] data;

        public ByteArray(int bits) {
            this.data = new byte[bits];
            Arrays.fill(this.data, Byte.MIN_VALUE);
        }

        public ByteArray(byte[] data) {
            this.data = data;
        }

        void set(int index, int value) {
            if (value < 0) {
                throw new RuntimeException("Can not set negative counts!");
            }
            int valueToSet = value + Byte.MIN_VALUE;
            if (valueToSet > Byte.MAX_VALUE) {
                throw new RuntimeException("Too large count " + value);
            }
            data[index] = max(data[index], (byte) valueToSet);
        }

        int get(int index) {
            return data[index] - Byte.MIN_VALUE;
        }

        /**
         * Number of bits
         */
        public int size() {
            return data.length;
        }

        /**
         * Number of set bits (1s)
         */
        int computeBitCount() {
            int bitCount = 0;
            for (byte aData : data) {
                if (aData != Byte.MIN_VALUE) {
                    bitCount++;
                }
            }
            return bitCount;
        }

        //Serialization

        public ByteArray() {
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public ByteArray mergeWith(ByteArray otherByteArray) {
            if (otherByteArray.size() != size()) {
                throw new RuntimeException("Unequal sizes!");
            }
            ByteArray result = new ByteArray(size());
            for (int i = 0; i < data.length; i++) {
                int sum = data[i] + otherByteArray.data[i] - Byte.MIN_VALUE;
                if (sum > Byte.MAX_VALUE) {
                    sum = Byte.MAX_VALUE;
                }
                result.data[i] = (byte) sum;
            }
            return result;
        }

        public ByteArray clone() {
            return new ByteArray(data.clone());
        }
    }
}
