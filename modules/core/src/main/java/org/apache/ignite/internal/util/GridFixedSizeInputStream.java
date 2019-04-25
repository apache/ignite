/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Input stream wrapper which allows to read exactly expected number of bytes.
 */
public class GridFixedSizeInputStream extends InputStream {
    /** */
    private final InputStream in;

    /** */
    private long size;

    /**
     * @param in Input stream.
     * @param size Size of available data.
     */
    public GridFixedSizeInputStream(InputStream in, long size) {
        this.in = in;
        this.size = size;
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        assert len <= b.length;

        if (size == 0)
            return -1;

        if (len > size)
            // Assignment is ok because size < len <= Integer.MAX_VALUE
            len = (int)size;

        int res = in.read(b, off, len);

        if (res == -1)
            throw new IOException("Expected " + size + " more bytes to read.");

        assert res >= 0 : res;

        size -= res;

        assert size >= 0 : size;

        return res;
    }

    /** {@inheritDoc} */
    @Override public int available() throws IOException {
        return size > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int)size;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        if (size == 0)
            return -1;

        int res = in.read();

        if (res == -1)
            throw new IOException("Expected " + size + " more bytes to read.");

        size--;

        return res;
    }
}