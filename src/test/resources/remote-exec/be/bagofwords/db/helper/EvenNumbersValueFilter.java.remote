package be.bagofwords.db.helper;

import be.bagofwords.exec.RemoteClass;

import java.io.Serializable;
import java.util.function.Predicate;

/**
 * Created by koen on 15/07/17.
 */
@RemoteClass
public class EvenNumbersValueFilter implements Predicate<Long>, Serializable {

    @Override
    public boolean test(Long value) {
        return value % 2 == 0;
    }
}
