package be.bow.db.combinator;

public class FloatCombinator implements Combinator<Float> {

    @Override
    public Float combine(Float first, Float second) {
        return first + second;
    }
}
