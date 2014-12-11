/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;

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
    @Override public void onExceptionCaught(GridNioSession ses, IgniteCheckedException ex) throws IgniteCheckedException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws IgniteCheckedException {
        // No encoding needed in direct mode.
        if (directMode)
            return proceedSessionWrite(ses, msg);

        try {
            ByteBuffer res = parser.encode(ses, msg);

            return proceedSessionWrite(ses, res);
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

                        LT.warn(log, null, "Parser returned null but there are still unread data in input buffer (bug in " +
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
