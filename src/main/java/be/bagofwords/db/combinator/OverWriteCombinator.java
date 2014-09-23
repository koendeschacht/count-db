package be.bagofwords.db.combinator;

public class OverWriteCombinator<T> implements Combinator<T> {

    @Override
    public T combine(T first, T second) {
        return second;
    }
}
