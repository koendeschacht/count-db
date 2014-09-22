package be.bow.main.bigrams;

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
}
