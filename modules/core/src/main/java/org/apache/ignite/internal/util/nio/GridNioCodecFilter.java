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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Filter that transforms byte buffers to user-defined objects and vice-versa with specified {@link GridNioParser}.
 */
public class GridNioCodecFilter extends GridNioFilterAdapter {
    /** Parser used. */
    private GridNioParser parser;

    /** Grid logger. */
    @GridToStringExclude
    private IgniteLogger log;

    /** Whether direct mode is used. */
    private boolean directMode;

    /**
     * Creates a codec filter.
     *
     * @param parser Parser to use.
     * @param log Log instance to use.
     * @param directMode Whether direct mode is used.
     */
    public GridNioCodecFilter(GridNioParser parser, IgniteLogger log, boolean directMode) {
        super("GridNioCodecFilter");

        this.parser = parser;
        this.log = log;
        this.directMode = directMode;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNioCodecFilter.class, this);
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
        // No encoding needed in direct mode.
        if (directMode)
            return proceedSessionWrite(ses, msg, fut, ackC);

        try {
            ByteBuffer res = parser.encode(ses, msg);

            return proceedSessionWrite(ses, res, fut, ackC);
        }
        catch (IOException e) {
            throw new GridNioException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws IgniteCheckedException {
        if (!(msg instanceof ByteBuffer))
            throw new GridNioException("Failed to decode incoming message (incoming message is not a byte buffer, " +
                "is filter properly placed?): " + msg.getClass());

        try {
            ByteBuffer input = (ByteBuffer)msg;

            while (input.hasRemaining()) {
                Object res = parser.decode(ses, input);

                if (res != null)
                    proceedMessageReceived(ses, res);
                else {
                    if (input.hasRemaining()) {
                        if (directMode)
                            return;

                        LT.warn(log, "Parser returned null but there are still unread data in input buffer (bug in " +
                            "parser code?) [parser=" + parser + ", ses=" + ses + ']');

                        input.position(input.limit());
                    }
                }
            }
        }
        catch (IOException e) {
            throw new GridNioException(e);
        }
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
