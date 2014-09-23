package be.bagofwords.db.combinator;

public interface Combinator<T extends Object> {

    T combine(T first, T second);

}
