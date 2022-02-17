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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.LimitedRateFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.LimitedRateFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertArrayEquals;

/**
 * Test {@link LimitedRateFileIO}.
 */
@RunWith(Parameterized.class)
public class LimitedRateFileIOTest extends GridCommonAbstractTest {
    /** Test buffer length. */
    private static final int DATA_LEN = 32 * 1024;

    /** Temp directory name. */
    private static final String TMP_DIR_NAME = "temp";

    /** Write to source channel flag. */
    @Parameterized.Parameter
    public boolean writeSrcChannel;

    /** Parameters. */
    @Parameterized.Parameters(name = "write={0}")
    public static Iterable<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        File tmpDir = new File(U.defaultWorkDirectory(), TMP_DIR_NAME);

        if (!tmpDir.exists())
            tmpDir.mkdirs();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        U.delete(new File(U.defaultWorkDirectory(), TMP_DIR_NAME));

        super.afterTestsStopped();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedTransfer() throws Exception {
        doLimitedChannelTransfer(4096, 65536, DATA_LEN);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedChannelTransferMinRate() throws Exception {
        doLimitedChannelTransfer(1024, 1024, 8192);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLimitedChannelTransferFewerDestBuf() throws Exception {
        doLimitedChannelTransfer(4096, 1024, DATA_LEN);
    }

    /**
     * @param rate Transfer rate in bytes/sec.
     * @param destBufLen Destination channel buffer length.
     * @param size Data length.
     * @throws Exception If failed.
     */
    private void doLimitedChannelTransfer(int rate, int destBufLen, int size) throws Exception {
        File tmpDir = new File(U.defaultWorkDirectory(), TMP_DIR_NAME);
        File srcFile = new File(tmpDir, U.maskForFileName(getClass().getSimpleName()) + ".tmp.1");
        byte[] srcData = new byte[size];

        ThreadLocalRandom.current().nextBytes(srcData);

        if (!writeSrcChannel) {
            try (FileOutputStream out = new FileOutputStream(srcFile)) {
                out.write(srcData);
            }
        }

        int expDuration = size * 1000 / rate;

        FileIOFactory ioFactory = new LimitedRateFileIOFactory(new RandomAccessFileIOFactory(), rate, rate);
        ByteArrayChannel testChannel = writeSrcChannel ?
            new ByteArrayChannel(destBufLen, srcData, srcData.length) : new ByteArrayChannel(destBufLen, (int)U.MB);

        long startTime = U.currentTimeMillis();

        OpenOption[] options = !writeSrcChannel ? new OpenOption[] {StandardOpenOption.READ} :
            new OpenOption[] {StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING};

        try (FileIO src = ioFactory.create(srcFile, options)) {
            if (writeSrcChannel)
                src.transferFrom(testChannel, 0, srcData.length);
            else
                src.transferTo(0, srcFile.length(), testChannel);
        }

        long delta = U.currentTimeMillis() - startTime;

        assertArrayEquals(srcData, writeSrcChannel ? FileUtils.readFileToByteArray(srcFile) : testChannel.data());

        int min = Math.round((float)expDuration / 2);
        int max = expDuration * 2;
        assertTrue("time=" + delta + ", min=" + min + ", max=" + max, delta > min && delta < max);
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
}
