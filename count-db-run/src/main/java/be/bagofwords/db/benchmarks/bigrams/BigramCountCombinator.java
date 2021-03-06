package be.bagofwords.db.benchmarks.bigrams;

import be.bagofwords.db.combinator.Combinator;
import be.bagofwords.exec.RemoteClass;

/**
 * Created by Koen Deschacht (koendeschacht@gmail.com) on 9/22/14.
 */
@RemoteClass
public class BigramCountCombinator implements Combinator<BigramCount> {

    @Override
    public BigramCount combine(BigramCount first, BigramCount second) {
        return new BigramCount(first.getBigram(), first.getCount() + second.getCount());
    }
}
