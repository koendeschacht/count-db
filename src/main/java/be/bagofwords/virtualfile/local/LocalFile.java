package be.bagofwords.virtualfile.local;


import be.bagofwords.virtualfile.VirtualFile;

import java.io.*;

public class LocalFile implements VirtualFile {

    private final File file;

    public LocalFile(File file) {
        this.file = file;
    }

    @Override
    public VirtualFile getFile(String relativePath) {
        File newFile = new File(file, relativePath);
        File parentFile = newFile.getParentFile();
        if (!parentFile.exists()) {
            boolean success = parentFile.mkdirs();
            if (!success) {
                throw new RuntimeException("Failed to create directory " + parentFile.getAbsolutePath());
            }
        }
        return new LocalFile(newFile);
    }

    @Override
    public InputStream createInputStream() {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to create inputstream for file " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public OutputStream createOutputStream() {
        try {
            return new FileOutputStream(file);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Failed to create inputstream for file " + file.getAbsolutePath(), e);
        }
    }

    @Override
    public boolean exists() {
        return file.exists();
    }
}
