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

package org.apache.ignite.internal.util.nio;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Verifies that first bytes received in accepted (incoming)
 * NIO session are equal to {@link U#IGNITE_HEADER}.
 * <p>
 * First {@code U.IGNITE_HEADER.length} bytes are consumed by this filter
 * and all other bytes are forwarded through chain without any modification.
 */
public class GridConnectionBytesVerifyFilter extends GridNioFilterAdapter {
    /** */
    private static final int MAGIC_META_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private static final int MAGIC_BUF_KEY = GridNioSessionMetaKey.nextUniqueKey();

    /** */
    private IgniteLogger log;

    /**
     * Creates a filter instance.
     *
     * @param log Logger.
     */
    public GridConnectionBytesVerifyFilter(IgniteLogger log) {
        super("GridConnectionBytesVerifyFilter");

        this.log = log;
    }

    /** {@inheritDoc} */
    @Override public void onSessionOpened(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(
        GridNioSession ses,
        IgniteCheckedException ex
    ) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(
        GridNioSession ses,
        Object msg,
        boolean fut,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        return proceedSessionWrite(ses, msg, fut, ackC);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        // Verify only incoming connections.
        if (!ses.accepted()) {
            proceedMessageReceived(ses, msg);

            return;
        }

        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Failed to decode incoming message (message should be a byte buffer, is " +
                "filter properly placed?): " + msg.getClass());

        ByteBuffer buf = (ByteBuffer)msg;

        Integer magic = ses.meta(MAGIC_META_KEY);

        if (magic == null || magic < U.IGNITE_HEADER.length) {
            byte[] magicBuf = ses.meta(MAGIC_BUF_KEY);

            if (magicBuf == null)
                magicBuf = new byte[U.IGNITE_HEADER.length];

            int magicRead = magic == null ? 0 : magic;

            int cnt = buf.remaining();

            buf.get(magicBuf, magicRead, Math.min(U.IGNITE_HEADER.length - magicRead, cnt));

            if (cnt + magicRead < U.IGNITE_HEADER.length) {
                // Magic bytes are not fully read.
                ses.addMeta(MAGIC_META_KEY, cnt + magicRead);
                ses.addMeta(MAGIC_BUF_KEY, magicBuf);
            }
            else if (U.bytesEqual(magicBuf, 0, U.IGNITE_HEADER, 0, U.IGNITE_HEADER.length)) {
                // Magic bytes read and equal to IGNITE_HEADER.
                ses.removeMeta(MAGIC_BUF_KEY);
                ses.addMeta(MAGIC_META_KEY, U.IGNITE_HEADER.length);

                proceedMessageReceived(ses, buf);
            }
            else {
                ses.close();

                LT.warn(log, "Unknown connection detected (is some other software connecting to this " +
                    "Ignite port?) [rmtAddr=" + ses.remoteAddress() + ", locAddr=" + ses.localAddress() + ']');
            }
        }
        else
            proceedMessageReceived(ses, buf);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws IgniteCheckedException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws IgniteCheckedException {
        proceedSessionWriteTimeout(ses);
    }
}
