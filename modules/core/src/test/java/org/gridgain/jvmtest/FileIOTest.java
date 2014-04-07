/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jvmtest;

import junit.framework.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Java file IO test.
 */
public class FileIOTest extends TestCase {
    /** File path. */
    private static final String FILE_PATH = "/test-java-file.tmp";

    /** Temp dir. */
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     * @throws Exception If failed.
     */
    public void testReadLineFromBinaryFile() throws Exception {
        File file = new File(FILE_PATH);

        file.deleteOnExit();

        RandomAccessFile raf = new RandomAccessFile(file, "rw");

        byte[] b = new byte[100];

        Arrays.fill(b, (byte)10);

        raf.write(b);

        raf.writeBytes("swap-spaces/space1/b53b3a3d6ab90ce0268229151c9bde11|" +
            "b53b3a3d6ab90ce0268229151c9bde11|1315392441288" + U.nl());

        raf.writeBytes("swap-spaces/space1/b53b3a3d6ab90ce0268229151c9bde11|" +
            "b53b3a3d6ab90ce0268229151c9bde11|1315392441288" + U.nl());

        raf.write(b);

        raf.writeBytes("test" + U.nl());

        raf.getFD().sync();

        raf.seek(0);

        while (raf.getFilePointer() < raf.length()) {
            String s = raf.readLine();

            X.println("String: " + s + ";");

            X.println("String length: " + s.length());

            X.println("File pointer: " + raf.getFilePointer());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleFilesCreation() throws Exception {
        File parent = new File(TMP_DIR, "testMultipleFilesCreation");

        U.delete(parent);

        U.mkdirs(parent);

        int childCnt = 4;
        int subChildCnt = 4;

        for (int i = 0; i < childCnt; i++) {
            File f = new File(parent, String.valueOf(i));

            U.mkdirs(f);

            for (int j = 0; j < subChildCnt; j++)
                U.mkdirs(new File(f, String.valueOf(j)));
        }

        X.println("Parent: " + parent.getAbsolutePath());
        X.println("Test started: " + U.format(System.currentTimeMillis()));

        long start = System.currentTimeMillis();

        byte[] data = new byte[4096];

        for (int i = 0; i < 50000; i++) {
            int idx1 = i % childCnt;
            int idx2 = (i / childCnt) % subChildCnt;

            RandomAccessFile f = null;

            try {
                f = new RandomAccessFile(new File(parent, idx1 + File.separator + idx2 + File.separatorChar + i), "rw");

                f.write(data);
            }
            finally {
                U.closeQuiet(f);
            }
        }

        X.println("Test time: " + (System.currentTimeMillis() - start));
    }

    /**
     *
     */
    public void testGetAbsolutePath() {
        for (int i = 0; i < 1000000; i++) {
            new File("/" + UUID.randomUUID().toString()).getAbsolutePath();

            new File(UUID.randomUUID().toString()).getAbsolutePath();

            new File("/Users").getAbsolutePath();
        }
    }
}
