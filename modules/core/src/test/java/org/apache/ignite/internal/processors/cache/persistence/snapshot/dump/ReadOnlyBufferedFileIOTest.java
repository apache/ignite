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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/***/
public class ReadOnlyBufferedFileIOTest extends GridCommonAbstractTest {
    /** */
    private static final File TEST_FILE = new File("test-read-only-file-io.dmp");

    /** */
    public static final int MIN_TEST_BUFFER_SIZE = 32;

    /** */
    public static final int TEST_BUFFER_SIZE = 64;

    /** */
    public static final int MAX_TEST_BUFFER_SIZE = 1024 * 1024 * 128;

    /** */
    @Test
    public void testOneSmallEntry() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE; bufSz <<= 1)
            check(bufSz, randBytes(10));
    }

    /** */
    private static byte[] randBytes(int cnt) {
        byte[] bytes = new byte[cnt];

        ThreadLocalRandom.current().nextBytes(bytes);

        return bytes;
    }

    /** */
    @Test
    public void testAFewSmallEntries() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE; bufSz <<= 1) {
            check(bufSz,
                randBytes(10),
                randBytes(bufSz >> 1),
                randBytes(bufSz >> 1)
            );
        }
    }

    /** */
    @Test
    public void testLargeEntry() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE >> 2; bufSz <<= 1)
            check(bufSz, randBytes((bufSz << 2) + 1));
    }

    /** */
    @Test
    public void testBufferSizeEntry() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE; bufSz <<= 1)
            check(bufSz, randBytes(bufSz));
    }

    /** */
    @Test
    public void testDoubleClose() throws IOException {
        check(TEST_BUFFER_SIZE, randBytes(10)).close();
    }

    /** */
    @Test
    public void testWrongArg() {
        assertThrows(
            null,
            () -> new ReadOnlyBufferedFileIO(null),
            IllegalArgumentException.class,
            "fileIO must not be null"
        );
    }

    /** */
    @Test
    public void testUnknownFSBlockSize() throws IOException {
        check(-1, randBytes(TEST_BUFFER_SIZE));

        check(0, randBytes(TEST_BUFFER_SIZE));
    }

    /** */
    private FileIO fileIOWithFSBlockSize(int blockSize) throws IOException {
        return new RandomAccessFileIO(TEST_FILE, CREATE, READ, WRITE) {
            @Override public int getFileSystemBlockSize() {
                return blockSize;
            }
        };
    }

    /** */
    private FileIO check(int bufSz, byte[]... data) throws IOException {
        if (TEST_FILE.exists() && !TEST_FILE.delete())
            throw new IgniteException(" Unable to delete " + TEST_FILE.getAbsolutePath());

        try (FileOutputStream fos = new FileOutputStream(TEST_FILE)) {
            for (byte[] b : data)
                fos.write(b);
        }

        try (FileIO fileIO = new ReadOnlyBufferedFileIO(fileIOWithFSBlockSize(bufSz))) {
            for (byte[] bytes : data) {
                int allocateLength = bytes.length + (data.length > 1 ? 0 : ThreadLocalRandom.current().nextInt(10));

                ByteBuffer readBuf = ByteBuffer.allocate(allocateLength);

                readBuf.limit(bytes.length);

                int len = fileIO.readFully(readBuf);

                assertEquals(bytes.length, len);

                byte[] arr = readBuf.array();

                if (arr.length != bytes.length)
                    arr = Arrays.copyOfRange(arr, 0, bytes.length);

                assertEqualsArraysAware(bytes, arr);
            }

            return fileIO;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        TEST_FILE.delete();
    }
}
