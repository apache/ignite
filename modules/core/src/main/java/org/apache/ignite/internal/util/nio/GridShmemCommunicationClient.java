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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryClientEndpoint;
import org.apache.ignite.internal.util.lang.IgniteInClosure2X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;

/**
 *
 */
public class GridShmemCommunicationClient extends GridAbstractCommunicationClient {
    /** */
    private final IpcSharedMemoryClientEndpoint shmem;

    /** */
    private final ByteBuffer writeBuf;

    /** */
    private final MessageFormatter formatter;

    /**
     * @param connIdx Connection index.
     * @param metricsLsnr Metrics listener.
     * @param port Shared memory IPC server port.
     * @param connTimeout Connection timeout.
     * @param log Logger.
     * @param formatter Message formatter.
     * @throws IgniteCheckedException If failed.
     */
    public GridShmemCommunicationClient(
        int connIdx,
        GridNioMetricsListener metricsLsnr,
        int port,
        long connTimeout,
        IgniteLogger log,
        MessageFormatter formatter)
        throws IgniteCheckedException
    {
        super(connIdx, metricsLsnr);

        assert metricsLsnr != null;
        assert port > 0 && port < 0xffff;
        assert connTimeout >= 0;

        shmem = new IpcSharedMemoryClientEndpoint(port, (int)connTimeout, log);

        writeBuf = ByteBuffer.allocate(8 << 10);

        writeBuf.order(ByteOrder.nativeOrder());

        this.formatter = formatter;
    }

    /** {@inheritDoc} */
    @Override public synchronized void doHandshake(IgniteInClosure2X<InputStream, OutputStream> handshakeC)
        throws IgniteCheckedException {
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
    @Override public synchronized void sendMessage(byte[] data, int len) throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Communication client was closed: " + this);

        try {
            shmem.outputStream().write(data, 0, len);

            metricsLsnr.onBytesSent(len);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send message to remote node: " + shmem, e);
        }

        markUsed();
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean sendMessage(UUID nodeId, Message msg,
        IgniteInClosure<IgniteException> closure) throws IgniteCheckedException {
        assert nodeId != null;

        if (closed())
            throw new IgniteCheckedException("Communication client was closed: " + this);

        assert writeBuf.hasArray();

        try {
            int cnt = U.writeMessageFully(msg, shmem.outputStream(), writeBuf, formatter.writer(nodeId));

            metricsLsnr.onBytesSent(cnt);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send message to remote node: " + shmem, e);
        }

        markUsed();

        if (closure != null)
            closure.apply(null);

        return false;
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridShmemCommunicationClient.class, this, super.toString());
    }
}
