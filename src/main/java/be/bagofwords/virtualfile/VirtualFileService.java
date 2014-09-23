package be.bagofwords.virtualfile;


import be.bagofwords.util.SerializationUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class VirtualFileService {

    public abstract VirtualFile getRootDirectory();

    public void storeSingleObject(String location, Object object) {
        VirtualFile file = getFile(location);
        try (OutputStream os = file.createOutputStream()) {
            SerializationUtils.writeObject(object, os);
        } catch (IOException exp) {
            throw new RuntimeException("Unexpected exception while trying to write object to " + location);
        }
    }

    public <T> T readSingleObject(String location, Class<T> _class) {
        VirtualFile file = getFile(location);
        try (InputStream is = file.createInputStream()) {
            return SerializationUtils.readObject(_class, is);
        } catch (IOException exp) {
            throw new RuntimeException("Unexpected exception while trying to write object to " + location);
        }
    }

    private VirtualFile getFile(String location) {
        return getRootDirectory().getFile(location);
    }
}
