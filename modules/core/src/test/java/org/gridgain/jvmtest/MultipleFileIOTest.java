/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.jvmtest;

import org.gridgain.grid.util.typedef.F;

import java.io.*;
import java.util.*;

/**
 *
 */
public class MultipleFileIOTest {
    /** Temp dir. */
    private static final String TMP_DIR = System.getProperty("java.io.tmpdir");

    /**
     *
     */
    private MultipleFileIOTest() {
        // No-op.
    }

    /**
     * @param args Args.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public static void main(String args[]) throws Exception {
        File parent = new File(TMP_DIR, "testMultipleFilesCreation");

        System.out.println("Deleting: " + new Date());

        delete(parent);
        parent.mkdirs();

        int childCnt = 10;
        int subChildCnt = 10;
        boolean useRaf = true;

        if (args.length > 0) {
            childCnt = Integer.parseInt(args[0]);
            subChildCnt = Integer.parseInt(args[1]);
        }

        if (args.length > 3)
            useRaf = Boolean.parseBoolean(args[2]);

        for (int i = 0; i < childCnt; i++) {
            File f = new File(parent, String.valueOf(i));

            f.mkdirs();

            for (int j = 0; j < subChildCnt; j++)
                new File(f, String.valueOf(j)).mkdirs();
        }

        System.out.println("Test started: " + new Date());
        System.out.println("Parent: " + parent.getAbsolutePath());

        long start = System.currentTimeMillis();

        byte[] data = new byte[4096];

        for (int i = 0; i < 50000; i++) {
            int idx1 = i % childCnt;
            int idx2 = (i / childCnt) % subChildCnt;

            File f = new File(parent, idx1 + File.separator + idx2 + File.separatorChar + i);

            if (useRaf) {
                RandomAccessFile raf = null;

                try {
                    raf = new RandomAccessFile(f, "rw");

                    raf.write(data);
                }
                finally {
                    if (raf != null)
                        raf.close();
                }
            }
            else {
                FileOutputStream fos = null;

                try {
                    fos = new FileOutputStream(f);

                    fos.write(data);
                }
                finally {
                    if (fos != null)
                        fos.close();
                }
            }
        }

        System.out.println("Test time: " + (System.currentTimeMillis() - start));
    }

    /**
     * @param f File to delete.
     */
    private static void delete(File f) {
        assert f != null;

        if (f.isDirectory()) {
            File[] files = f.listFiles();

            if (!F.isEmpty(files)) {
                for (File f0 : files)
                    delete(f0);
            }
        }

        f.delete();
    }
}
