package be.bagofwords.main.tests.bigrams;

import be.bagofwords.db.combinator.Combinator;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
public class BigramCountCombinator implements Combinator<BigramCount> {

    @Override
    public BigramCount combine(BigramCount first, BigramCount second) {
        return new BigramCount(first.getBigram(), first.getCount() + second.getCount());
    }
}
