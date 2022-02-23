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

package org.apache.ignite.internal.util.io;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.LimitedWriteRateFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.LimitedWriteRateFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.junit.Assert.assertArrayEquals;

/**
 * Test {@link LimitedWriteRateFileIO}.
 */
@RunWith(Parameterized.class)
public class LimitedWriteRateFileIOTest extends GridCommonAbstractTest {
    /** Test buffer length. */
    private static final int DATA_LEN = 32 * 1024 + 2;

    /** Temp directory name. */
    private static final String TMP_DIR_NAME = "temp";

    /** Temp file. */
    private static File tempFile;

    /** Write rate. */
    @Parameterized.Parameter(0)
    public int rate;

    /** Destination buffer length. */
    @Parameterized.Parameter(1)
    public int bufLen;

    /** Data length. */
    @Parameterized.Parameter(2)
    public int dataLen;

    /** Parameters. */
    @Parameterized.Parameters(name = "rate={0}, bufLen={1}, dataLen={2}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[] {4096, 65536, DATA_LEN});
        params.add(new Object[] {1024, 1024, 8192});
        params.add(new Object[] {4096, 1024, DATA_LEN});

        return params;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        File tmpDir = new File(U.defaultWorkDirectory(), TMP_DIR_NAME);

        if (!tmpDir.exists())
            tmpDir.mkdirs();

        tempFile = new File(new File(U.defaultWorkDirectory(), TMP_DIR_NAME),
            U.maskForFileName(getClass().getSimpleName()) + ".tmp.1");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        U.delete(new File(U.defaultWorkDirectory(), TMP_DIR_NAME));

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        tempFile.delete();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedTransferTo() throws Exception {
        ByteArrayChannel destChannel = new ByteArrayChannel(bufLen, (int)U.MB);
        byte[] srcData = generateData(dataLen);
        int offset = dataLen / 2;

        FileUtils.writeByteArrayToFile(tempFile, srcData);

        checkExecTime((src) -> src.transferTo(offset, srcData.length - offset, destChannel), offset, READ);

        assertArrayEquals(Arrays.copyOfRange(srcData, offset, srcData.length), destChannel.data());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedTransferFrom() throws Exception {
        byte[] srcData = generateData(dataLen);
        int offset = dataLen / 2;

        if (offset > 0)
            FileUtils.writeByteArrayToFile(tempFile, Arrays.copyOfRange(srcData, 0, offset));

        ByteArrayChannel srcChannel =
            new ByteArrayChannel(bufLen, Arrays.copyOfRange(srcData, offset, dataLen), dataLen - offset);

        checkExecTime((dest) -> dest.transferFrom(srcChannel, offset, dataLen - offset), offset, CREATE, WRITE, APPEND);

        assertArrayEquals(srcData, FileUtils.readFileToByteArray(tempFile));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedBytebufferWrite() throws Exception {
        byte[] srcData = generateData(dataLen);
        int offset = dataLen / 2;

        if (offset > 0)
            FileUtils.writeByteArrayToFile(tempFile, Arrays.copyOfRange(srcData, 0, offset));

        checkExecTime((dest) -> offset == 0 ? dest.write(ByteBuffer.wrap(srcData)) :
            dest.write(ByteBuffer.wrap(Arrays.copyOfRange(srcData, offset, dataLen)), offset), offset, CREATE, WRITE, APPEND);

        assertArrayEquals(srcData, FileUtils.readFileToByteArray(tempFile));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedWrite() throws Exception {
        byte[] srcData = generateData(dataLen);
        int offset = dataLen / 2;

        if (offset > 0)
            FileUtils.writeByteArrayToFile(tempFile, Arrays.copyOfRange(srcData, 0, offset));

        checkExecTime((dest) -> dest.write(srcData, offset, dataLen - offset), offset, CREATE, WRITE, APPEND);

        assertArrayEquals(srcData, FileUtils.readFileToByteArray(tempFile));
    }

    private byte[] generateData(int len) {
        byte[] data = new byte[len];

        ThreadLocalRandom.current().nextBytes(data);

        return data;
    }

    private void checkExecTime(IoFunction func, int offset, OpenOption... modes) throws IOException {
        LimitedWriteRateFileIOFactory ioFactory =
            new LimitedWriteRateFileIOFactory(new RandomAccessFileIOFactory(), rate, rate);

        int payloadLen = dataLen - offset;

        try (FileIO io = ioFactory.create(tempFile, modes)) {
            long startTime = System.currentTimeMillis();

            long written = func.apply(io);

            long delta = System.currentTimeMillis() - startTime;

            assertEquals(payloadLen, written);

            long expDuration = TimeUnit.SECONDS.toMillis(payloadLen / rate);
            long min = Math.round((double)expDuration / 2);
            long max = expDuration * 2;

            assertTrue("time=" + delta + ", min=" + min + ", max=" + max, delta > min && delta < max);
        }
    }

    /** */
    static class ByteArrayChannel implements WritableByteChannel, ReadableByteChannel {
        /** */
        private final byte[] data;

        /** */
        private final int destBufCapacity;

        /** */
        private int size;

        /** */
        private int pos;

        /**
         * @param destBufCapacity Max internal buffer size.
         */
        public ByteArrayChannel(int destBufCapacity, int capacity) {
            this(destBufCapacity, new byte[capacity], 0);
        }

        /**
         * @param destBufCapacity Max internal buffer size.
         */
        public ByteArrayChannel(int destBufCapacity, byte[] src, int size) {
            this.destBufCapacity = destBufCapacity;
            this.size = size;

            data = src;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer src) throws IOException {
            int len = Math.min(src.remaining(), destBufCapacity);

            src.get(data, pos, len);

            size += len;

            pos = size;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer dst) throws IOException {
            int len = Math.min(Math.min(size - pos, destBufCapacity), dst.remaining());

            dst.put(data, pos, len);

            pos += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public boolean isOpen() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            // No-op.
        }

        /**
         * @return Internal data buffer copy.
         */
        public byte[] data() {
            return Arrays.copyOf(data, size);
        }
    }

    /** I/O function.*/
    private interface IoFunction {
        /**
         * @param fileIo I/O file interface to use.
         * @return Number of written bytes.
         * @throws IOException If failed.
         */
        long apply(FileIO fileIo) throws IOException;
    }
}
