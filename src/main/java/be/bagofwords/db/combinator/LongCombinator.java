package be.bagofwords.db.combinator;

public class LongCombinator implements Combinator<Long> {

    @Override
    public Long combine(Long first, Long second) {
        return first + second;
    }
}
