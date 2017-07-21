package be.bagofwords.db.combinator;

import be.bagofwords.exec.RemoteObjectConfig;

import java.io.Serializable;

public interface Combinator<T extends Object> extends Serializable {

    T combine(T first, T second);

    default RemoteObjectConfig createExecConfig() {
        return RemoteObjectConfig.create(this).add(getClass());
    }
}
