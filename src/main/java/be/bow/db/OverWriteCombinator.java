package be.bow.db;

public class OverWriteCombinator<T> implements Combinator<T> {

    @Override
    public T combine(T first, T second) {
        return second;
    }
}
