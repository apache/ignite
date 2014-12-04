/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.nio;

import org.gridgain.grid.*;
import org.gridgain.grid.util.direct.*;
import org.jetbrains.annotations.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * Grid client for NIO server.
 */
public class GridTcpNioCommunicationClient extends GridAbstractCommunicationClient {
    /** Socket. */
    private final GridNioSession ses;

    /**
     * Constructor for test purposes only.
     */
    public GridTcpNioCommunicationClient() {
        super(null);

        ses = null;
    }

    /**
     * @param ses Session.
     */
    public GridTcpNioCommunicationClient(GridNioSession ses) {
        super(null);

        assert ses != null;

        this.ses = ses;
    }

    /**
     * @return Gets underlying session.
     */
    public GridNioSession session() {
        return ses;
    }

    /** {@inheritDoc} */
    @Override public void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC) throws GridException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        boolean res = super.close();

        if (res)
            ses.close();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        super.forceClose();

        ses.close();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(byte[] data, int len) throws GridException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws GridException {
        if (closed())
            throw new GridException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(data);

        if (fut.isDone()) {
            try {
                fut.get();
            }
            catch (IOException e) {
                throw new GridException("Failed to send message [client=" + this + ']', e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg)
        throws GridException {
        // Node ID is never provided in asynchronous send mode.
        assert nodeId == null;

        if (closed())
            throw new GridException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(msg);

        if (fut.isDone()) {
            try {
                fut.get();
            }
            catch (IOException e) {
                throw new GridException("Failed to send message [client=" + this + ']', e);
            }
        }
    }

    /**
     * @param timeout Timeout.
     * @throws IOException If failed.
     */
    @Override public void flushIfNeeded(long timeout) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean async() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public long getIdleTime() {
        long now = U.currentTimeMillis();

        // Session can be used for receiving and sending.
        return Math.min(Math.min(now - ses.lastReceiveTime(), now - ses.lastSendScheduleTime()),
            now - ses.lastSendTime());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpNioCommunicationClient.class, this, super.toString());
    }
}
