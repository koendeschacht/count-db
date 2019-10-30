package be.bagofwords.db.combinator;

import be.bagofwords.exec.RemoteObjectConfig;

import java.io.Serializable;

public interface Combinator<T extends Object> extends Serializable {

    T combine(T first, T second);

    default void addRemoteClasses(RemoteObjectConfig objectConfig) {
        //Don't add any classes by default
    }

    default RemoteObjectConfig createExecConfig() {
        RemoteObjectConfig result = RemoteObjectConfig.create(this);
        result.add(getClass());
        addRemoteClasses(result);
        return result;
    }
}
