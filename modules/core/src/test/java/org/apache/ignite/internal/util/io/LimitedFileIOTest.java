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
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.LimitedFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.nio.file.StandardOpenOption.READ;
import static org.apache.ignite.internal.util.IgniteUtils.KB;
import static org.junit.Assert.assertArrayEquals;

public class LimitedFileIOTest extends GridCommonAbstractTest {

    private static final int DATA_LEN = 32 * (int)U.KB;

    private static final String TMP_DIR = "temp";

    private static File srcFile;

    private static byte[] srcData;

    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        File tmpDir = new File(U.defaultWorkDirectory(), TMP_DIR);

        if (!tmpDir.exists())
            tmpDir.mkdirs();

        srcFile = new File(tmpDir, U.maskForFileName(getClass().getSimpleName()) + ".tmp.1");
        srcData = new byte[DATA_LEN];

        ThreadLocalRandom.current().nextBytes(srcData);

        try (FileOutputStream out = new FileOutputStream(srcFile)) {
            out.write(srcData);
        }
    }

    @Override protected void afterTestsStopped() throws Exception {
        U.delete(new File(U.defaultWorkDirectory(), TMP_DIR));

        super.afterTestsStopped();
    }

    @Test
    public void testLimitedChannelTransfer() throws IOException {
        int rate = 4 * (int)KB;

        doLimitedChannelTransfer(rate, rate);
    }

    // todo too long
    @Test
    public void testLimitedChannelTransferMinRate() throws IOException {
        int rate = (int)KB;

        doLimitedChannelTransfer(rate, rate);
    }

    @Test
    public void testLimitedChannelTransferFewerDestBuf() throws IOException {
        int rate = 4 * (int)KB;

        doLimitedChannelTransfer(rate, 1);
    }

    private void doLimitedChannelTransfer(int rateBytes, int blockLen) throws IOException {
        int expDuration = DATA_LEN / rateBytes;

        FileIOFactory ioFactory = new LimitedFileIOFactory(new RandomAccessFileIOFactory(), rateBytes / (int)KB);

        ByteArrayWritableChannel testChannel = new ByteArrayWritableChannel(blockLen);

        long startTime = U.currentTimeMillis();

        try (FileIO src = ioFactory.create(srcFile, READ)) {
            src.transferTo(0, srcFile.length(), testChannel);
        }

        double deltaMs = U.currentTimeMillis() - startTime;

        assertArrayEquals(srcData, testChannel.data());
        assertTrue("time(ms) = " + deltaMs, (int)Math.round(deltaMs / 1000) >= Math.round((float)expDuration / 2));
    }

    @Test
    public void testAsyncChannel() throws IOException {
//        AsyncFileIOFactory fact = new AsyncFileIOFactory();
    }

    static class ByteArrayWritableChannel implements WritableByteChannel {
        private final byte[] data = new byte[(int)U.MB];

        private final int blockSize;

        private int position;

        /**
         * @param blockSize Max internal buffer size.
         */
        public ByteArrayWritableChannel(int blockSize) {
            this.blockSize = blockSize;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer src) throws IOException {
            int len = Math.min(src.remaining(), blockSize);

            src.get(data, position, len);

            position += len;

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

        public byte[] data() {
            return Arrays.copyOf(data, position);
        }
    }
}
