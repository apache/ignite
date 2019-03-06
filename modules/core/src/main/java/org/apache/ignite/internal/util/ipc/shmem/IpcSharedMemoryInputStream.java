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

package org.apache.ignite.internal.util.ipc.shmem;

import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class IpcSharedMemoryInputStream extends InputStream {
    /** */
    private final IpcSharedMemorySpace in;

    /** Stream instance is not thread-safe so we can cache buffer. */
    private byte[] buf = new byte[1];

    /**
     * @param in Space.
     */
    public IpcSharedMemoryInputStream(IpcSharedMemorySpace in) {
        assert in != null;

        this.in = in;
    }

    /** {@inheritDoc} */
    @Override public int read() throws IOException {
        try {
            int read = in.read(buf, 0, 1, 0);

            if (read < 0)
                return read;

            return buf[0] & 0xFF;
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int read(byte[] b, int off, int len) throws IOException {
        try {
            return in.read(b, off, len, 0);
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public int available() throws IOException {
        try {
            return in.unreadCount();
        }
        catch (IgniteCheckedException e) {
            throw new IOException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws IOException {
        in.close();
    }

    /**
     * Forcibly closes spaces and frees all system resources.
     * <p>
     * This method should be called with caution as it may result to the other-party
     * process crash. It is intended to call when there was an IO error during handshake
     * and other party has not yet attached to the space.
     */
    public void forceClose() {
        in.forceClose();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IpcSharedMemoryInputStream.class, this);
    }
}