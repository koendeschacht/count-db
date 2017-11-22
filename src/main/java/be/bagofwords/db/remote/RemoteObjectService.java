package be.bagofwords.db.remote;

import be.bagofwords.exec.PackedRemoteObject;
import be.bagofwords.exec.RemoteObjectUtil;

import java.util.HashMap;
import java.util.Map;

public class RemoteObjectService {

    private Map<PackedRemoteObject, Object> cachedObjects = new HashMap<>();

    public synchronized Object loadObject(PackedRemoteObject packedRemoteObject) {
        Object cachedObj = cachedObjects.get(packedRemoteObject);
        if (cachedObj == null) {
            cachedObj = RemoteObjectUtil.loadObject(packedRemoteObject);
            cachedObjects.put(packedRemoteObject, cachedObj);
        }
        return cachedObj;
    }

}
