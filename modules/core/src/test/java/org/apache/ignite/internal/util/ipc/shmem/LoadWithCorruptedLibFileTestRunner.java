package org.apache.ignite.internal.util.ipc.shmem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class LoadWithCorruptedLibFileTestRunner {
    public static final String TMP_DIR_FOR_TEST = System.getProperty("user.home");
    public static final String LOADED_LIB_FILE_NAME = System.mapLibraryName(IpcSharedMemoryNativeLoader.LIB_NAME);

    public static void main(String[] args) throws Exception {
        System.setProperty("java.io.tmpdir", TMP_DIR_FOR_TEST);

        createCorruptedLibFile();

        IpcSharedMemoryNativeLoader.load();
    }

    private static void createCorruptedLibFile() throws IOException {
        File libFile = new File(System.getProperty("java.io.tmpdir"), LOADED_LIB_FILE_NAME);

        if (libFile.exists() && ! libFile.delete())
            throw new IllegalStateException("Could not delete loaded lib file.");

        libFile.deleteOnExit();

        if (! libFile.createNewFile())
            throw new IllegalStateException("Could not create new file.");

        try (FileOutputStream out = new FileOutputStream(libFile)) {
            out.write("Corrupted file information instead of good info.\n".getBytes());
        }
    }
}
