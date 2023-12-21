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
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/***/
public class WriteOnlyBufferedFileIOTest extends GridCommonAbstractTest {
    /** */
    private static final File TEST_FILE = new File("test-write-only-file-io.dmp");

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
            check(bufSz, ByteBuffer.wrap(randBytes(10)));
    }

    /** */
    private static byte[] randBytes(int cnt) {
        byte[] bytes = new byte[cnt];

        new Random().nextBytes(bytes);

        return bytes;
    }

    /** */
    @Test
    public void testAFewSmallEntries() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE; bufSz <<= 1) {
            check(bufSz,
                ByteBuffer.wrap(randBytes(10)),
                ByteBuffer.wrap(randBytes(bufSz >> 1)),
                ByteBuffer.wrap(randBytes(bufSz >> 1))
            );
        }
    }

    /** */
    @Test
    public void testLargeEntry() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE >> 2; bufSz <<= 1)
            check(bufSz, ByteBuffer.wrap(randBytes(bufSz << 2 + 1)));
    }

    /** */
    @Test
    public void testBufferSizeEntry() throws IOException {
        for (int bufSz = MIN_TEST_BUFFER_SIZE; bufSz <= MAX_TEST_BUFFER_SIZE; bufSz <<= 1)
            check(bufSz, ByteBuffer.wrap(randBytes(bufSz)));
    }

    /** */
    @Test
    public void testDoubleClose() throws IOException {
        FileIO fileIO = check(TEST_BUFFER_SIZE, ByteBuffer.wrap(randBytes(10)));

        byte[] content1 = Files.readAllBytes(TEST_FILE.toPath());

        fileIO.close();

        assertEqualsArraysAware(content1, Files.readAllBytes(TEST_FILE.toPath()));
    }

    /** */
    @Test
    public void testWrongArg() {
        assertThrows(
            null,
            () -> new WriteOnlyBufferedFileIOFactory(0),
            IllegalArgumentException.class,
            "bufSz must be positive"
        );

        assertThrows(
            null,
            () -> new WriteOnlyBufferedFileIOFactory(-1),
            IllegalArgumentException.class,
            "bufSz must be positive"
        );
    }

    /** */
    private FileIO check(int bufSz, ByteBuffer... data) throws IOException {
        if (TEST_FILE.exists() && !TEST_FILE.delete())
            throw new IgniteException(" Unable to delete " + TEST_FILE.getAbsolutePath());

        WriteOnlyBufferedFileIOFactory factory = new WriteOnlyBufferedFileIOFactory(bufSz);

        FileIO fileIO = factory.create(TEST_FILE);

        ByteBuffer expectedData = ByteBuffer.allocate(Arrays.stream(data).mapToInt(Buffer::remaining).sum());

        Arrays.stream(data).forEach(bb -> expectedData.put(bb.duplicate()));

        Arrays.stream(data).forEach(bb -> {
            try {
                fileIO.writeFully(bb);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        fileIO.close();

        assertEquals(expectedData.limit(), TEST_FILE.length());

        assertEqualsArraysAware(expectedData.array(), Files.readAllBytes(TEST_FILE.toPath()));

        return fileIO;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        TEST_FILE.delete();
    }
}
