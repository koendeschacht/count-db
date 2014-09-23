package be.bagofwords.db.combinator;

public class DoubleCombinator implements Combinator<Double> {

    @Override
    public Double combine(Double first, Double second) {
        return first + second;
    }
}
