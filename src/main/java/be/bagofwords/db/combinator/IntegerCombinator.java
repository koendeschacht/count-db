package be.bagofwords.db.combinator;

public class IntegerCombinator implements Combinator<Integer> {

    @Override
    public Integer combine(Integer first, Integer second) {
        return first + second;
    }
}
