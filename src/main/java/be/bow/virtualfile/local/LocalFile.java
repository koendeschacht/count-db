package be.bow.virtualfile.local;


import be.bow.virtualfile.VirtualFile;

import java.io.*;

public class LocalFile implements VirtualFile {

    private final File file;

    public LocalFile(File file) {
        this.file = file;
    }

    @Override
    public VirtualFile getFile(String relativePath) {
        File newFile = new File(file, relativePath);
        newFile.getParentFile().mkdirs(); //Ignore result, might exist already
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
