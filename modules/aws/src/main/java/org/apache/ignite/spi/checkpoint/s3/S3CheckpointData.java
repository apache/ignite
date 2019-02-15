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

package org.apache.ignite.spi.checkpoint.s3;

import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Wrapper of all checkpoint that are saved to the S3. It
 * extends every checkpoint with expiration time and host name
 * which created this checkpoint.
 * <p>
 * Host name is used by {@link S3CheckpointSpi} SPI to give node
 * correct files if it is restarted.
 */
class S3CheckpointData {
    /** Checkpoint data. */
    private final byte[] state;

    /** Checkpoint expiration time. */
    private final long expTime;

    /** Checkpoint key. */
    private final String key;

    /**
     * Creates new instance of checkpoint data wrapper.
     *
     * @param state Checkpoint data.
     * @param expTime Checkpoint expiration time in milliseconds.
     * @param key Key of checkpoint.
     */
    S3CheckpointData(byte[] state, long expTime, String key) {
        assert expTime >= 0;

        this.state = state;
        this.expTime = expTime;
        this.key = key;
    }

    /**
     * Gets checkpoint data.
     *
     * @return Checkpoint data.
     */
    byte[] getState() {
        return state;
    }

    /**
     * Gets checkpoint expiration time.
     *
     * @return Expire time in milliseconds.
     */
    long getExpireTime() {
        return expTime;
    }

    /**
     * Gets key of checkpoint.
     *
     * @return Key of checkpoint.
     */
    public String getKey() {
        return key;
    }

    /**
     * @return Serialized checkpoint data.
     */
    public byte[] toBytes() {
        byte[] keyBytes = key.getBytes();

        byte[] bytes = new byte[4 + state.length + 8 + 4 + keyBytes.length];

        U.intToBytes(state.length, bytes, 0);
        U.arrayCopy(state, 0, bytes, 4, state.length);
        U.longToBytes(expTime, bytes, 4 + state.length);
        U.intToBytes(keyBytes.length, bytes, 4 + state.length + 8);
        U.arrayCopy(keyBytes, 0, bytes, 4 + state.length + 8 + 4, keyBytes.length);

        return bytes;
    }

    /**
     * @param in Input stream.
     * @return Checkpoint data.
     * @throws IOException In case of error.
     */
    public static S3CheckpointData fromStream(InputStream in) throws IOException {
        byte[] buf = new byte[8];

        read(in, buf, 4);

        byte[] state = new byte[U.bytesToInt(buf, 0)];

        read(in, state, state.length);

        read(in, buf, 8);

        long expTime = U.bytesToLong(buf, 0);

        read(in, buf, 4);

        byte[] keyBytes = new byte[U.bytesToInt(buf, 0)];

        read(in, keyBytes, keyBytes.length);

        return new S3CheckpointData(state, expTime, new String(keyBytes));
    }

    /**
     * @param in Input stream.
     * @param buf Buffer.
     * @param len Number of bytes to read.
     * @throws IOException In case of error.
     */
    private static void read(InputStream in, byte[] buf, int len) throws IOException {
        int cnt = in.read(buf, 0, len);

        if (cnt < len)
            throw new IOException("End of stream reached.");
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(S3CheckpointData.class, this);
    }
}