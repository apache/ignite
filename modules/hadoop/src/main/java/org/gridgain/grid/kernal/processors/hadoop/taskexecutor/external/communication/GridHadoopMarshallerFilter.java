/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.taskexecutor.external.communication;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.hadoop.message.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.util.nio.*;

/**
 * Serialization filter.
 */
public class GridHadoopMarshallerFilter extends GridNioFilterAdapter {
    /** Marshaller. */
    private GridMarshaller marshaller;

    /**
     * @param marshaller Marshaller to use.
     */
    public GridHadoopMarshallerFilter(GridMarshaller marshaller) {
        super("GridHadoopMarshallerFilter");

        this.marshaller = marshaller;
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
        assert msg instanceof GridHadoopMessage : "Invalid message type: " + msg;

        return proceedSessionWrite(ses, marshaller.marshal(msg));
    }

    @Override public void onMessageReceived(GridNioSession ses, Object msg) throws GridException {
        assert msg instanceof byte[];

        // Always unmarshal with system classloader.
        proceedMessageReceived(ses, marshaller.unmarshal((byte[])msg, null));
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
