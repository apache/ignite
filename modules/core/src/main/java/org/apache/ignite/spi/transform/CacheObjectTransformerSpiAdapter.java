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

package org.apache.ignite.spi.transform;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.ThreadLocalDirectByteBuffer;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
@IgniteExperimental
public abstract class CacheObjectTransformerSpiAdapter extends IgniteSpiAdapter implements CacheObjectTransformerSpi {
    /** Source byte buffer. */
    private final ThreadLocalDirectByteBuffer srcBuf = new ThreadLocalDirectByteBuffer();

    /** Destination byte buffer. */
    private final ThreadLocalDirectByteBuffer dstBuf = new ThreadLocalDirectByteBuffer();

    /** {@inheritDoc} */
    @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
        startStopwatch();

        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** */
    private ByteBuffer sourceByteBuffer(byte[] bytes, int offset, int length) {
        ByteBuffer src;

        if (direct()) {
            src = srcBuf.get(bytes.length);

            src.put(bytes, offset, length);
            src.flip();
        }
        else
            src = ByteBuffer.wrap(bytes, offset, length);

        return src;
    }

    /** Thread local direct byte buffer with a required capacty. */
    protected ByteBuffer byteBuffer(int capacity) {
        ByteBuffer buf;

        log.info("Buffer Capacity=" + capacity); // TODO
        ((IgniteLoggerEx)log).flush();

        if (direct()) {
            buf = dstBuf.get(capacity);

            buf.limit(capacity);
        }
        else
            buf = ByteBuffer.wrap(new byte[capacity]);

        log.info("Buffer ready"); // TODO
        ((IgniteLoggerEx)log).flush();

        return buf;
    }

    /** {@inheritDoc} */
    @Override public byte[] transform(byte[] bytes, int offset, int length) throws IgniteCheckedException {
        ByteBuffer src = sourceByteBuffer(bytes, offset, length);
        ByteBuffer transformed = transform(src);

        assert transformed.remaining() > 0 : transformed.remaining();

        byte[] res = new byte[OVERHEAD + transformed.remaining()];

        if (transformed.isDirect())
            transformed.get(res, OVERHEAD, transformed.remaining());
        else {
            byte[] arr = transformed.array();

            U.arrayCopy(arr, 0, res, OVERHEAD, arr.length);
        }

        return res;
    }

    /**
     * Transforms the data.
     *
     * @param original Original data.
     * @return Transformed data.
     * @throws IgniteCheckedException when transformation is not possible/suitable.
     */
    protected abstract ByteBuffer transform(ByteBuffer original) throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public byte[] restore(byte[] bytes, int offset, int length) {
        ByteBuffer src = sourceByteBuffer(bytes, offset, bytes.length - offset);
        ByteBuffer restored = restore(src, length);

        assert restored.remaining() == length : "expected=" + length + ", actual=" + restored.remaining();

        if (restored.isDirect()) {
            byte[] res = new byte[length];

            restored.get(res);

            return res;
        }
        else
            return restored.array();
    }

    /**
     * Restores the data.
     *
     * @param transformed Transformed data.
     * @param length Original data length.
     * @return Restored data.
     */
    protected abstract ByteBuffer restore(ByteBuffer transformed, int length);

    /**
     * Returns {@code true} when direct byte buffers are required.
     */
    protected boolean direct() {
        return false;
    }
}
