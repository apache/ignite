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

package org.apache.ignite.internal.processors.cache.persistence.wal.mmap.optane;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.persistence.wal.mmap.ByteBufferHolder;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 *  Tests PMDK backed buffers.
 */
public class OptaneBuffersTest extends GridCommonAbstractTest {
    /** */
    private static final String IGNITE_TEST_PMEP = "IGNITE_TEST_PMEM";

    /** */
    private static final int BUF_SZ = 4096;

    /** */
    private File workDir;

    /** */
    private final boolean isPmem = IgniteSystemProperties.getBoolean(IGNITE_TEST_PMEP, false);

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        String path = U.defaultWorkDirectory();

        workDir = U.resolveWorkDirectory(path, "mmap", true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        U.delete(workDir);
    }

    /** */
    @Test
    public void testCheckIfPmem() throws Exception {
        log.info("Working on " + workDir.getAbsolutePath() + ": isPmem=" + isPmem);
        assertEquals(isPmem, OptaneUtil.isPersistentMemory(workDir.getAbsolutePath()));
    }

    /** */
    @Test
    public void testMapUnmap() {
        Path filePath = getTestFilePath();

        AtomicReference<OptaneByteBufferHolder> bufRef = new AtomicReference<>();
        try (OptaneByteBufferHolder bufHolder = createMmapBuffer(filePath, BUF_SZ)) {
            bufRef.set(bufHolder);

            if (isPmem)
                assertEquals(ByteBufferHolder.Type.OPTANE, bufHolder.type());
            else
                assertEquals(ByteBufferHolder.Type.MMAP, bufHolder.type());

            assertTrue(bufHolder.buffer().isDirect());
            assertEquals(BUF_SZ, bufHolder.buffer().capacity());
        }

        GridTestUtils.assertThrows(log(), () -> bufRef.get().msync(), RuntimeException.class,
            "Buffer has been already closed.");
    }

    /** */
    @Test
    public void testCheckMsyncBoundaries() {
        Path filePath = getTestFilePath();

        try (OptaneByteBufferHolder bufHolder = createMmapBuffer(filePath, BUF_SZ)) {
            Stream.of(new T2<>(-10, 100), new T2<>(-10, -100), new T2<>(0, BUF_SZ + 1), new T2<>(BUF_SZ, BUF_SZ + 1))
                .forEach((pair) -> {
                    int offset = pair.getKey();
                    int length = pair.getValue();

                    GridTestUtils.assertThrowsWithCause(
                        () -> bufHolder.msync(offset, length), IndexOutOfBoundsException.class
                    );
                });
        }
    }

    /** */
    @Test
    public void checkMsync() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        byte[] data = new byte[BUF_SZ >> 3];
        rnd.nextBytes(data);

        int offset = BUF_SZ >> 5;
        Path filePath = getTestFilePath();

        try (OptaneByteBufferHolder bufHolder = createMmapBuffer(filePath, BUF_SZ)) {
            ByteBuffer buf = bufHolder.buffer();

            buf.position(offset);
            buf.put(data);

            // Check that data synced to FS after msync.
            bufHolder.msync(offset, data.length);
            checkData(filePath, data, offset);

            // Put large data at same position, doesn't sync before unmapping.
            data = new byte[data.length << 1];
            rnd.nextBytes(data);
            buf.position(offset);
            buf.put(data);
        }
        checkData(filePath, data, offset);
    }

    /** */
    private Path getTestFilePath() {
        return Paths.get(workDir.getAbsolutePath(), UUID.randomUUID() + ".wal");
    }

    /** */
    private OptaneByteBufferHolder createMmapBuffer(Path file, long bufSz) {
        Context ctx = OptaneUtil.mmap(file.toFile().getAbsolutePath(), bufSz);

        return new OptaneByteBufferHolder(ctx);
    }

    /** */
    private void checkData(Path path, byte[] data, long offset) throws Exception {
        try (FileInputStream stream = new FileInputStream(path.toFile())) {
            assert stream.skip(offset) == offset;

            byte[] buf = new byte[data.length];

            assertTrue(stream.read(buf) > 0);
            assertArrayEquals(buf, data);
        }
    }
}
