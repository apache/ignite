/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.jvmtest;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

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