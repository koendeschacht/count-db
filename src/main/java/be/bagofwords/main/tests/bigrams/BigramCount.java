package be.bagofwords.main.tests.bigrams;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class BigramCount {
    private long bigram;
    private long count;

    public BigramCount(long bigram) {
        this(bigram, 1);
    }

    public BigramCount(long bigram, long count) {
        this.bigram = bigram;
        this.count = count;
    }

    public long getBigram() {
        return bigram;
    }

    public long getCount() {
        return count;
    }


    //Used for serialization

    public BigramCount() {
    }

    //Used for serialization

    public void setBigram(long bigram) {
        this.bigram = bigram;
    }

    //Used for serialization

    public void setCount(long count) {
        this.count = count;
    }
}
