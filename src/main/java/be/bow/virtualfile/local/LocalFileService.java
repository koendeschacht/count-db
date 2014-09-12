package be.bow.virtualfile.local;

import be.bow.virtualfile.VirtualFile;
import be.bow.virtualfile.VirtualFileService;

import java.io.File;

public class LocalFileService extends VirtualFileService {

    private File rootDirectory;

    public LocalFileService(String rootDirectory) {
        this.rootDirectory = new File(rootDirectory);
        if (this.rootDirectory.exists()) {
            if (!this.rootDirectory.isDirectory()) {
                throw new RuntimeException("Expected " + this.rootDirectory.getAbsolutePath() + " to be a directory");
            }
        } else {
            boolean success = this.rootDirectory.mkdirs();
            if (!success) {
                throw new RuntimeException("Failed to created directory " + this.rootDirectory.getAbsolutePath());
            }
        }
    }

    @Override
    public VirtualFile getRootDirectory() {
        return new LocalFile(rootDirectory);
    }
}
