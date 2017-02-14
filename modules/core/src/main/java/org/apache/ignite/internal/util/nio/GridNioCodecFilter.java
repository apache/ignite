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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Filter that transforms byte buffers to user-defined objects and vice-versa
 * with specified {@link GridNioParser}.
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
        boolean fut
    ) throws IgniteCheckedException {
        // No encoding needed in direct mode.
        if (directMode)
            return proceedSessionWrite(ses, msg, fut);

        try {
            ByteBuffer res = parser.encode(ses, msg);

            return proceedSessionWrite(ses, res, fut);
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
