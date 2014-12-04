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

import java.nio.*;

/**
 * Verifies that first bytes received in accepted (incoming)
 * NIO session are equal to {@link U#GG_HEADER}.
 * <p>
 * First {@code U.GG_HEADER.length} bytes are consumed by this filter
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
    @Override public void onSessionOpened(GridNioSession ses) throws GridException {
        proceedSessionOpened(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionClosed(GridNioSession ses) throws GridException {
        proceedSessionClosed(ses);
    }

    /** {@inheritDoc} */
    @Override public void onExceptionCaught(GridNioSession ses, GridException ex) throws GridException {
        proceedExceptionCaught(ses, ex);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<?> onSessionWrite(GridNioSession ses, Object msg) throws GridException {
        return proceedSessionWrite(ses, msg);
    }

    /** {@inheritDoc} */
    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
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

        if (magic == null || magic < U.GG_HEADER.length) {
            byte[] magicBuf = ses.meta(MAGIC_BUF_KEY);

            if (magicBuf == null)
                magicBuf = new byte[U.GG_HEADER.length];

            int magicRead = magic == null ? 0 : magic;

            int cnt = buf.remaining();

            buf.get(magicBuf, magicRead, Math.min(U.GG_HEADER.length - magicRead, cnt));

            if (cnt + magicRead < U.GG_HEADER.length) {
                // Magic bytes are not fully read.
                ses.addMeta(MAGIC_META_KEY, cnt + magicRead);
                ses.addMeta(MAGIC_BUF_KEY, magicBuf);
            }
            else if (U.bytesEqual(magicBuf, 0, U.GG_HEADER, 0, U.GG_HEADER.length)) {
                // Magic bytes read and equal to GG_HEADER.
                ses.removeMeta(MAGIC_BUF_KEY);
                ses.addMeta(MAGIC_META_KEY, U.GG_HEADER.length);

                proceedMessageReceived(ses, buf);
            }
            else {
                ses.close();

                LT.warn(log, null, "Unknown connection detected (is some other software connecting to this " +
                    "GridGain port?) [rmtAddr=" + ses.remoteAddress() + ", locAddr=" + ses.localAddress() + ']');
            }
        }
        else
            proceedMessageReceived(ses, buf);
    }

    /** {@inheritDoc} */
    @Override public GridNioFuture<Boolean> onSessionClose(GridNioSession ses) throws GridException {
        return proceedSessionClose(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionIdleTimeout(GridNioSession ses) throws GridException {
        proceedSessionIdleTimeout(ses);
    }

    /** {@inheritDoc} */
    @Override public void onSessionWriteTimeout(GridNioSession ses) throws GridException {
        proceedSessionWriteTimeout(ses);
    }
}
