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
import java.nio.channels.Channel;
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
    /** Estimated duration of an I/O operation. */
    private static final int DURATION_SEC = 4;

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

    /** Flag for writing only part of the data to check the write with an offset. */
    @Parameterized.Parameter(2)
    public boolean checkOffset;

    /** Binary data. */
    private byte[] data;

    /** File offset. */
    private int offset;

    /** Parameters. */
    @Parameterized.Parameters(name = "rate={0}, bufLen={1}, checkOffset={2}")
    public static Iterable<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        params.add(new Object[] {4096, 65536, true});
        params.add(new Object[] {4096, 65536, false});
        params.add(new Object[] {4096, 1, true});
        params.add(new Object[] {4096, 1, false});
        params.add(new Object[] {1, 1, true});
        params.add(new Object[] {1, 1, false});

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

        int len = DURATION_SEC * rate + 1;;
        offset = checkOffset ? len : 0;
        data = new byte[len + offset];

        ThreadLocalRandom.current().nextBytes(data);

        log.info("Test params: length=" + data.length + ", offset=" + offset);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedTransferTo() throws Exception {
        WritableByteArrayChannel destChannel = new WritableByteArrayChannel(bufLen, data.length);

        FileUtils.writeByteArrayToFile(tempFile, data);

        checkExecTime((src) -> src.transferTo(offset, data.length - offset, destChannel), READ);

        assertArrayEquals(Arrays.copyOfRange(data, offset, data.length), destChannel.data());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedTransferFrom() throws Exception {
        checkLimitedWrite((dest) -> {
            ReadableByteArrayChannel srcChannel =
                new ReadableByteArrayChannel(bufLen, Arrays.copyOfRange(data, offset, data.length));

            return dest.transferFrom(srcChannel, offset, data.length - offset);
        });
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedBufferWrite() throws Exception {
        checkLimitedWrite((dest) -> offset == 0 ? dest.write(ByteBuffer.wrap(data)) :
            dest.write(ByteBuffer.wrap(Arrays.copyOfRange(data, offset, data.length)), offset));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedWrite() throws Exception {
        checkLimitedWrite((dest) -> dest.write(data, offset, data.length - offset));
    }

    /**
     * @param op I/O operation.
     * @throws IOException if failed.
     */
    private void checkLimitedWrite(TestIOOperation op) throws IOException {
        if (offset > 0)
            FileUtils.writeByteArrayToFile(tempFile, Arrays.copyOfRange(data, 0, offset));

        checkExecTime(op, CREATE, WRITE, APPEND);

        assertArrayEquals(data, FileUtils.readFileToByteArray(tempFile));
    }

    /**
     * @param op I/O operation.
     * @param modes Open modes.
     * @throws IOException if failed.
     */
    private void checkExecTime(TestIOOperation op, OpenOption... modes) throws IOException {
        LimitedWriteRateFileIOFactory ioFactory =
            new LimitedWriteRateFileIOFactory(new RandomAccessFileIOFactory(), rate, rate);

        try (FileIO fileIo = ioFactory.create(tempFile, modes)) {
            long startTime = System.currentTimeMillis();

            long written = op.run(fileIo);

            long delta = System.currentTimeMillis() - startTime;

            assertEquals(data.length - offset, written);

            long expDuration = TimeUnit.SECONDS.toMillis(DURATION_SEC);
            long min = Math.round((double)expDuration / 2);
            long max = expDuration * 2;

            assertTrue("time=" + delta + ", min=" + min + ", max=" + max, delta > min && delta < max);
        }
    }

    /** Byte array channel. */
    private abstract static class ByteArrayChannel implements Channel {
        /** Data buffer. */
        protected byte[] data;

        /** Single operation buffer capacity. */
        protected int opBufCap;

        /** Buffer limit. */
        protected int limit;

        /** {@inheritDoc} */
        @Override public boolean isOpen() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            // No-op.
        }

        /** @return Internal data buffer copy. */
        public byte[] data() {
            return Arrays.copyOf(data, limit);
        }
    }

    /** */
    private static class WritableByteArrayChannel extends ByteArrayChannel implements WritableByteChannel {
        /**
         * @param opBufCap Single operation buffer capacity.
         * @param capacity Internal buffer capacity.
         */
        public WritableByteArrayChannel(int opBufCap, int capacity) {
            this.opBufCap = opBufCap;

            data = new byte[capacity];
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer src) throws IOException {
            int len = Math.min(src.remaining(), opBufCap);

            src.get(data, limit, len);

            limit += len;

            return len;
        }
    }

    /** */
    private static class ReadableByteArrayChannel extends ByteArrayChannel implements ReadableByteChannel {
        /** Buffer position. */
        private int pos;

        /**
         * @param opBufCap Single operation buffer capacity.
         * @param data Binary data to read from the channel.
         */
        protected ReadableByteArrayChannel(int opBufCap, byte[] data) {
            this.opBufCap = opBufCap;
            this.data = data;

            limit = data.length;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer dst) throws IOException {
            int len = Math.min(Math.min(limit - pos, opBufCap), dst.remaining());

            dst.put(data, pos, len);

            pos += len;

            return len;
        }
    }

    /** Abstract I/O operation. */
    private interface TestIOOperation {
        /**
         * @param fileIo I/O file interface to use.
         * @return Number of written bytes.
         * @throws IOException If failed.
         */
        long run(FileIO fileIo) throws IOException;
    }
}
