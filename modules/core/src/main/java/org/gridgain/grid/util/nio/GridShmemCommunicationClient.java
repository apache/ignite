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

package org.gridgain.grid.util.nio;

import org.apache.ignite.*;
import org.apache.ignite.internal.util.direct.*;
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
     * @throws IgniteCheckedException If failed.
     */
    public GridShmemCommunicationClient(GridNioMetricsListener metricsLsnr, int port, long connTimeout, IgniteLogger log,
        GridNioMessageWriter msgWriter)
        throws IgniteCheckedException {
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
    @Override public synchronized boolean sendMessage(@Nullable UUID nodeId, GridTcpCommunicationMessageAdapter msg)
        throws IgniteCheckedException {
        if (closed())
            throw new IgniteCheckedException("Communication client was closed: " + this);

        assert writeBuf.hasArray();

        try {
            int cnt = msgWriter.writeFully(nodeId, msg, shmem.outputStream(), writeBuf);

            metricsLsnr.onBytesSent(cnt);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to send message to remote node: " + shmem, e);
        }

        markUsed();

        return false;
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ByteBuffer data) throws IgniteCheckedException {
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
