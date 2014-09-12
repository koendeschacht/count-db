package be.bow.db;

public interface Combinator<T extends Object> {

    T combine(T first, T second);

}
