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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;

/**
 * File IO tests.
 */
public class IgniteFileIOTest extends TestCase {
    /** Test data size. */
    private static final int TEST_DATA_SIZE = 16 * 1024 * 1024;

    /**
     *
     */
    private static class TestFileIO extends AbstractFileIO {
        /** Data. */
        private final byte[] data;
        /** Position. */
        private int position;

        /**
         * @param maxSize Maximum size.
         */
        TestFileIO(int maxSize) {
            this.data = new byte[maxSize];
        }

        /**
         * @param data Initial data.
         */
        TestFileIO(byte[] data) {
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public long position() throws IOException {
            return position;
        }

        /** {@inheritDoc} */
        @Override public void position(long newPosition) throws IOException {
            checkPosition(newPosition);

            this.position = (int)newPosition;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf) throws IOException {
            final int len = Math.min(destBuf.remaining(), data.length - position);

            destBuf.put(data, position, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int read(ByteBuffer destBuf, long position) throws IOException {
            checkPosition(position);

            final int len = Math.min(destBuf.remaining(), data.length - (int)position);

            destBuf.put(data, (int)position, len);

            return len;
        }

        /** {@inheritDoc} */
        @Override public int read(byte[] buf, int off, int maxLen) throws IOException {
            final int len = Math.min(maxLen, data.length - position);

            System.arraycopy(data, position, buf, off, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf) throws IOException {
            final int len = Math.min(srcBuf.remaining(), data.length - position);

            srcBuf.get(data, position, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            checkPosition(position);

            final int len = Math.min(srcBuf.remaining(), data.length - (int)position);

            srcBuf.get(data, (int)position, len);

            return len;
        }

        /** {@inheritDoc} */
        @Override public int write(byte[] buf, int off, int maxLen) throws IOException {
            final int len = Math.min(maxLen, data.length - position);

            System.arraycopy(buf, off, data, position, len);

            position += len;

            return len;
        }

        /** {@inheritDoc} */
        @Override public MappedByteBuffer map(int sizeBytes) throws IOException {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void force() throws IOException {
        }

        /** {@inheritDoc} */
        @Override public void force(boolean withMetadata) throws IOException {
        }

        /** {@inheritDoc} */
        @Override public long size() throws IOException {
            return data.length;
        }

        /** {@inheritDoc} */
        @Override public void clear() throws IOException {
            position = 0;
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
        }

        /**
         * @param position Position.
         */
        private void checkPosition(long position) throws IOException {
            if (position < 0 || position >= data.length)
                throw new IOException("Invalid position: " + position);
        }
    }

    /**
     * test for 'full read' functionality.
     */
    public void testReadFully() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        fillRandomArray(arr);

        ByteBuffer buf = ByteBuffer.allocate(TEST_DATA_SIZE);

        TestFileIO fileIO = new TestFileIO(arr) {
            @Override public int read(ByteBuffer destBuf) throws IOException {
                if (destBuf.remaining() < 2)
                    return super.read(destBuf);

                int oldLimit = destBuf.limit();

                destBuf.limit(destBuf.position() + (destBuf.remaining() >> 1));

                try {
                    return super.read(destBuf);
                }
                finally {
                    destBuf.limit(oldLimit);
                }
            }
        };

        fileIO.readFully(buf);

        assert buf.remaining() == 0;

        assert compareArrays(arr, buf.array());
    }

    /**
     * test for 'full read' functionality.
     */
    public void testReadFullyArray() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        byte[] arrDst = new byte[TEST_DATA_SIZE];

        fillRandomArray(arr);

        TestFileIO fileIO = new TestFileIO(arr) {
            @Override public int read(byte[] buf, int off, int len) throws IOException {
                return super.read(buf, off, len < 2 ? len : (len >> 1));
            }
        };

        fileIO.readFully(arrDst, 0, arrDst.length);

        assert compareArrays(arr, arrDst);
    }

    /**
     * test for 'full write' functionality.
     */
    public void testWriteFully() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        ByteBuffer buf = ByteBuffer.allocate(TEST_DATA_SIZE);

        fillRandomArray(buf.array());

        TestFileIO fileIO = new TestFileIO(arr) {
            @Override public int write(ByteBuffer destBuf) throws IOException {
                if (destBuf.remaining() < 2)
                    return super.write(destBuf);

                int oldLimit = destBuf.limit();

                destBuf.limit(destBuf.position() + (destBuf.remaining() >> 1));

                try {
                    return super.write(destBuf);
                }
                finally {
                    destBuf.limit(oldLimit);
                }
            }
        };

        fileIO.writeFully(buf);

        assert buf.remaining() == 0;

        assert compareArrays(arr, buf.array());
    }

    /**
     * test for 'full write' functionality.
     */
    public void testWriteFullyArray() throws Exception {
        byte[] arr = new byte[TEST_DATA_SIZE];

        byte[] arrSrc = new byte[TEST_DATA_SIZE];

        fillRandomArray(arrSrc);

        TestFileIO fileIO = new TestFileIO(arr) {
            @Override public int write(byte[] buf, int off, int len) throws IOException {
                return super.write(buf, off, len < 2 ? len : (len >> 1));
            }
        };

        fileIO.writeFully(arrSrc, 0, arrSrc.length);

        assert compareArrays(arr, arrSrc);
    }

    /**
     * @param arr Array.
     */
    private static void fillRandomArray(@NotNull final byte[] arr) {
        ThreadLocalRandom.current().nextBytes(arr);
    }

    /**
     * @param arr1 Array 1.
     * @param arr2 Array 2.
     */
    private static boolean compareArrays(@NotNull final byte[] arr1, @NotNull final byte[] arr2) {
        if (arr1.length != arr2.length)
            return false;

        for (int i = 0; i < arr1.length; i++)
            if (arr1[i] != arr2[i])
                return false;

        return true;
    }
}
