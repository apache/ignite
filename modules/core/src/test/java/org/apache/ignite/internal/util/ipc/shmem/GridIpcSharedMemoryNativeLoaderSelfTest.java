package org.apache.ignite.internal.util.ipc.shmem;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class GridIpcSharedMemoryNativeLoaderSelfTest extends TestCase {
    private static final String DEFAULT_TMP_DIR = System.getProperty("java.io.tmpdir");
    public static final String TMP_DIR_FOR_TEST = System.getProperty("user.home");
    public static final String LOADED_FILE_NAME = System.mapLibraryName(GridIpcSharedMemoryNativeLoader.libFileName());

    @Override
    public void setUp() throws Exception {
        System.setProperty("java.io.tmpdir", TMP_DIR_FOR_TEST);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        System.setProperty("java.io.tmpdir", DEFAULT_TMP_DIR);
    }

    //TODO linux specific
    public void testLoadIfLibFileWasCorrupted() throws Exception {
        createCorruptedLibFile();

        GridIpcSharedMemoryNativeLoader.load();
    }

    private void createCorruptedLibFile() throws IOException {
        File loadedFile = new File(System.getProperty("java.io.tmpdir"), LOADED_FILE_NAME);

        if (loadedFile.exists())
            assertTrue("Could not delete libggshem file.",loadedFile.delete());
        loadedFile.deleteOnExit();

        assertTrue("Could not create new file.", loadedFile.createNewFile());

        try (FileOutputStream out = new FileOutputStream(loadedFile)){
            out.write("Corrupted information.\n".getBytes());
        };
    }
}