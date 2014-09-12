package be.bow.db.bloomfilter;

import be.bow.util.HashUtils;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.io.Serializable;
import java.math.RoundingMode;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

@JsonIgnoreProperties("dataCheckSum")
public class LongBloomFilter implements Serializable {

    private int numOfHashFunctions;
    private BitArray bits;

    public LongBloomFilter(long expectedSize, double fpp) {
        if (expectedSize > Integer.MAX_VALUE) {
            throw new RuntimeException("Creating a bloomfilter currently not supported for size " + expectedSize);
        }
        if (expectedSize == 0) {
            expectedSize = 100;
        }
        long numBits = optimalNumOfBits(expectedSize, fpp);
        this.bits = new BitArray(numBits);
        this.numOfHashFunctions = optimalNumOfHashFunctions(expectedSize, numBits);
    }

    public LongBloomFilter(BitArray bitArray, int numOfHashFunctions) {
        this.bits = bitArray;
        this.numOfHashFunctions = numOfHashFunctions;
    }

    public boolean mightContain(long hash64) {
        hash64 = HashUtils.randomDistributeHash(hash64);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        for (int i = 1; i <= numOfHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            if (!bits.get(nextHash % bits.size())) {
                return false;
            }
        }
        return true;
    }

    public <T> boolean put(long hash64) {
        hash64 = HashUtils.randomDistributeHash(hash64);
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);
        boolean bitsChanged = false;
        for (int i = 1; i <= numOfHashFunctions; i++) {
            int nextHash = hash1 + i * hash2;
            if (nextHash < 0) {
                nextHash = ~nextHash;
            }
            bitsChanged |= bits.set(nextHash % bits.size());
        }
        return bitsChanged;
    }

    private static long optimalNumOfBits(long n, double fpp) {
        if (fpp == 0) {
            fpp = Double.MIN_VALUE;
        }
        return (long) (-n * Math.log(fpp) / (Math.log(2) * Math.log(2)));
    }

    private static int optimalNumOfHashFunctions(long n, long m) {
        return Math.max(1, (int) Math.round(m / n * Math.log(2)));
    }

    //Used for serialization
    public LongBloomFilter() {
    }

    public int getNumOfHashFunctions() {
        return numOfHashFunctions;
    }

    public void setNumOfHashFunctions(int numOfHashFunctions) {
        this.numOfHashFunctions = numOfHashFunctions;
    }


    public double expectedFpp() {
        return Math.pow((double) bits.getBitCount() / bits.size(), numOfHashFunctions);
    }


    public LongBloomFilter clone() {
        return new LongBloomFilter(getBits().clone(), numOfHashFunctions);
    }

    //Json serialization

    public BitArray getBits() {
        return bits;
    }

    public void setBits(BitArray bits) {
        this.bits = bits;
    }

    public static class LongFunnel implements Funnel<Long> {
        public void funnel(Long s, PrimitiveSink primitiveSink) {
            primitiveSink.putLong(s);
        }
    }

    public static class BitArray {
        private long[] data;
        private int bitCount;

        BitArray(long bits) {
            this(new long[Ints.checkedCast(LongMath.divide(bits, 64, RoundingMode.CEILING))]);
        }

        // Used by serialization
        BitArray(long[] data) {
            checkArgument(data.length > 0, "data length is zero!");
            this.data = data;
            int bitCount = 0;
            for (long value : data) {
                bitCount += Long.bitCount(value);
            }
            this.bitCount = bitCount;
        }

        //Serialization
        public BitArray() {
        }

        public BitArray clone() {
            return new BitArray(data.clone());
        }

        /**
         * Returns true if the bit changed value.
         */
        boolean set(int index) {
            if (!get(index)) {
                data[index >> 6] |= (1L << index);
                bitCount++;
                return true;
            }
            return false;
        }

        boolean get(int index) {
            return (data[index >> 6] & (1L << index)) != 0;
        }

        /**
         * Number of bits
         */
        public int size() {
            return data.length * Long.SIZE;
        }

        /**
         * Number of set bits (1s)
         */
        int getBitCount() {
            return bitCount;
        }

        public void setBitCount(int bitCount) {
            this.bitCount = bitCount;
        }

        public long[] getData() {
            return data;
        }

        public void setData(long[] data) {
            this.data = data;
        }

        BitArray copy() {
            return new BitArray(data.clone());
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof BitArray) {
                BitArray bitArray = (BitArray) o;
                return Arrays.equals(data, bitArray.data);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(data);
        }


        public BitArray mergeWith(BitArray otherBitArray) {
            if (otherBitArray.size() != size()) {
                throw new RuntimeException("Unequal sizes!");
            }
            BitArray result = new BitArray(size());
            int bitCount = 0;
            for (int i = 0; i < data.length; i++) {
                result.data[i] = data[i] | otherBitArray.data[i];
                bitCount += Long.bitCount(result.data[i]);
            }
            result.bitCount = bitCount;
            return result;
        }
    }

}
