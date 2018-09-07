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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public abstract class AbstractFileIO implements FileIO {
    /** Max io timeout milliseconds. */
    private static final int MAX_IO_TIMEOUT_MS = 2000;

    /**
     *
     */
    private interface IOOperation {
        /**
         * @param offs Offset.
         *
         * @return Number of bytes operated.
         */
        public int run(int offs) throws IOException;
    }

    /**
     * @param operation IO operation.
     *
     * @param num Number of bytes to operate.
     */
    private int fully(IOOperation operation, long position, int num, boolean write) throws IOException {
        if (num > 0) {
            long time = 0;

            for (int i = 0; i < num; ) {
                int n = operation.run(i);

                if (n > 0) {
                    i += n;
                    time = 0;
                }
                else if (n == 0) {
                    if (!write && available(num - i, position + i) == 0)
                        return i;

                    if (time == 0)
                        time = U.currentTimeMillis();
                    else if ((U.currentTimeMillis() - time) >= MAX_IO_TIMEOUT_MS)
                        throw new IOException(write && (position + i) == size() ? "Failed to extend file." :
                            "Probably disk is too busy, please check your device.");
                }
                else
                    return -1;
            }
        }

        return num;
    }

    /** {@inheritDoc} */
    @Override public int readFully(final ByteBuffer destBuf) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return read(destBuf);
            }
        }, position(), destBuf.remaining(), false);
    }

    /** {@inheritDoc} */
    @Override public int readFully(final ByteBuffer destBuf, final long position) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return read(destBuf, position + offs);
            }
        }, position, destBuf.remaining(), false);
    }

    /** {@inheritDoc} */
    @Override public int readFully(final byte[] buf, final int off, final int len) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return read(buf, off + offs, len - offs);
            }
        }, position(), len, false);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(final ByteBuffer srcBuf) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return write(srcBuf);
            }
        }, position(), srcBuf.remaining(), true);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(final ByteBuffer srcBuf, final long position) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return write(srcBuf, position + offs);
            }
        }, position, srcBuf.remaining(), true);
    }

    /** {@inheritDoc} */
    @Override public int writeFully(final byte[] buf, final int off, final int len) throws IOException {
        return fully(new IOOperation() {
            @Override public int run(int offs) throws IOException {
                return write(buf, off + offs, len - offs);
            }
        }, position(), len, true);
    }

    /**
     * @param requested Requested.
     * @param position Position.
     *
     * @return Bytes available.
     */
    private int available(int requested, long position) throws IOException {
        long avail = size() - position;

        return requested > avail ? (int) avail : requested;
    }

}
