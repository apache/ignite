/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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