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

package org.apache.ignite.internal.util.nio.ssl;

import java.nio.ByteBuffer;
import javax.net.ssl.SSLEngine;

/**
 *
 */
public class GridSslMeta {
    /** GridNioSslHandler. */
    private GridNioSslHandler hnd;

    /** Data already decoded by blocking ssl handler. */
    private ByteBuffer decodedBuf;

    /** Data read but not decoded by blocking ssl handler. */
    private ByteBuffer encodedBuf;

    /** Ssl engine. */
    private SSLEngine sslEngine;

    /**
     *
     */
    public SSLEngine sslEngine() {
        return sslEngine;
    }

    /**
     * @param sslEngine Ssl engine.
     */
    public void sslEngine(SSLEngine sslEngine) {
        this.sslEngine = sslEngine;
    }

    /**
     *
     */
    public GridNioSslHandler handler() {
        return hnd;
    }

    /**
     * @param hnd Handler.
     */
    public void handler(GridNioSslHandler hnd) {
        this.hnd = hnd;
    }

    /**
     *
     */
    public ByteBuffer decodedBuffer() {
        return decodedBuf;
    }

    /**
     * @param decodedBuf Decoded buffer.
     */
    public void decodedBuffer(ByteBuffer decodedBuf) {
        this.decodedBuf = decodedBuf;
    }

    /**
     *
     */
    public ByteBuffer encodedBuffer() {
        return encodedBuf;
    }

    /**
     * @param encodedBuf Encoded buffer.
     */
    public void encodedBuffer(ByteBuffer encodedBuf) {
        this.encodedBuf = encodedBuf;
    }
}
