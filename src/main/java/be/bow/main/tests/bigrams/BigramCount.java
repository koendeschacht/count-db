package be.bow.main.tests.bigrams;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class BigramCount {
    private String firstWord;
    private String secondWord;
    private long count;

    public BigramCount(String firstWord, String secondWord) {
        this(firstWord, secondWord, 1);
    }

    public BigramCount(String firstWord, String secondWord, long count) {
        this.firstWord = firstWord;
        this.secondWord = secondWord;
        this.count = count;
    }

    public String getFirstWord() {
        return firstWord;
    }

    public String getSecondWord() {
        return secondWord;
    }

    public long getCount() {
        return count;
    }


    //Used for serialization

    public BigramCount() {
    }

    //Used for serialization

    public void setFirstWord(String firstWord) {
        this.firstWord = firstWord;
    }

    //Used for serialization

    public void setSecondWord(String secondWord) {
        this.secondWord = secondWord;
    }

    //Used for serialization

    public void setCount(long count) {
        this.count = count;
    }
}
