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
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.ipc.shmem.*;
import org.jetbrains.annotations.*;
import org.gridgain.grid.util.lang.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 *
 */
public class GridShmemCommunicationClient extends GridAbstractCommunicationClient {
    /** */
    private final GridIpcSharedMemoryClientEndpoint shmem;

    /** */
    private final ByteBuffer writeBuf;

    /** */
    private final GridNioMessageWriter msgWriter;

    /**
     * @param metricsLsnr Metrics listener.
     * @param port Shared memory IPC server port.
     * @param connTimeout Connection timeout.
     * @param log Logger.
     * @param msgWriter Message writer.
     * @throws GridException If failed.
     */
    public GridShmemCommunicationClient(GridNioMetricsListener metricsLsnr, int port, long connTimeout, IgniteLogger log,
        GridNioMessageWriter msgWriter)
        throws GridException {
        super(metricsLsnr);

        assert metricsLsnr != null;
        assert msgWriter != null;
        assert port > 0 && port < 0xffff;
        assert connTimeout >= 0;

        shmem = new GridIpcSharedMemoryClientEndpoint(port, (int)connTimeout, log);

        this.msgWriter = msgWriter;

        writeBuf = ByteBuffer.allocate(8 << 10);

        writeBuf.order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override public  synchronized void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC)
        throws GridException {
        handshakeC.applyx(shmem.inputStream(), shmem.outputStream());
    }

    /** {@inheritDoc} */
    @Override public boolean close() {
        boolean res = super.close();

        if (res)
            shmem.close();

        return res;
    }

    /** {@inheritDoc} */
    @Override public void forceClose() {
        super.forceClose();

        // Do not call forceClose() here.
        shmem.close();
    }

    /** {@inheritDoc} */
    @Override public synchronized void sendMessage(byte[] data, int len) throws GridException {
        if (closed())
            throw new GridException("Communication client was closed: " + this);

        try {
            shmem.outputStream().write(data, 0, len);

            metricsLsnr.onBytesSent(len);
        }
        catch (IOException e) {
            throw new GridException("Failed to send message to remote node: " + shmem, e);
        }

        markUsed();
    }

    /** {@inheritDoc} */
    @Override public synchronized void sendMessage(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg)
        throws GridException {
        if (closed())
            throw new GridException("Communication client was closed: " + this);

        assert writeBuf.hasArray();

        try {
            int cnt = msgWriter.writeFully(nodeId, msg, shmem.outputStream(), writeBuf);

            metricsLsnr.onBytesSent(cnt);
        }
        catch (IOException e) {
            throw new GridException("Failed to send message to remote node: " + shmem, e);
        }

        markUsed();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws GridException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public void flushIfNeeded(long timeout) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridShmemCommunicationClient.class, this, super.toString());
    }
}
