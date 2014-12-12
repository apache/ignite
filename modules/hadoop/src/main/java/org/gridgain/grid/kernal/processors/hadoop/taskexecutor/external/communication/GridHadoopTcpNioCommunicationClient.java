/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.*;
import org.gridgain.grid.util.nio.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Grid client for NIO server.
 */
public class GridHadoopTcpNioCommunicationClient extends GridHadoopAbstractCommunicationClient {
    /** Socket. */
    private final GridNioSession ses;

    /**
     * Constructor for test purposes only.
     */
    public GridHadoopTcpNioCommunicationClient() {
        ses = null;
    }

    /**
     * @param ses Session.
     */
    public GridHadoopTcpNioCommunicationClient(GridNioSession ses) {
        assert ses != null;

        this.ses = ses;
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
    @Override public void sendMessage(GridHadoopProcessDescriptor desc, GridHadoopMessage msg)
        throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Client was closed: " + this);

        GridNioFuture<?> fut = ses.send(msg);

        if (fut.isDone()) {
            try {
                fut.get();
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to send message [client=" + this + ']', e);
            }
        }
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
        return S.toString(GridHadoopTcpNioCommunicationClient.class, this, super.toString());
    }
}
