package be.bagofwords.virtualfile;

import java.io.InputStream;
import java.io.OutputStream;

public interface VirtualFile {

    public VirtualFile getFile(String relativePath);

    public InputStream createInputStream();

    public OutputStream createOutputStream();

    public boolean exists();
}
