/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.communication.tcp.internal.shmem;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.shmem.IpcSharedMemoryServerEndpoint;
import org.apache.ignite.internal.util.nio.GridNioMessageReaderFactory;
import org.apache.ignite.internal.util.nio.GridNioMessageWriterFactory;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.thread.IgniteThread;

/**
 * This worker takes request created via shmem.
 */
public class ShmemAcceptWorker extends GridWorker {
    /** Worker name. */
    private static final String WORKER_NAME = "shmem-communication-acceptor";

    /** Shared memory workers. */
    private final Collection<ShmemWorker> shmemWorkers = new ConcurrentLinkedDeque<>();

    /** Server. */
    private final IpcSharedMemoryServerEndpoint srv;

    /** Server listener. */
    private final GridNioServerListener<Message> srvLsnr;

    /** Statistics. */
    private final TcpCommunicationMetricsListener metricsLsnr;

    /** Logger. */
    private final IgniteLogger log;

    /** Message factory. */
    private final MessageFactory msgFactory;

    /** Writer factory. */
    private final GridNioMessageWriterFactory writerFactory;

    /** Reader factory. */
    private final GridNioMessageReaderFactory readerFactory;

    /** Tracing. */
    private final Tracing tracing;

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param srvLsnr Server listener.
     * @param srv Server.
     * @param metricsLsnr Metrics listener.
     * @param log Logger.
     * @param msgFactory Message factory.
     * @param writerFactory Writer factory.
     * @param readerFactory Reader factory.
     * @param tracing Tracing.
     */
    public ShmemAcceptWorker(
        String igniteInstanceName,
        GridNioServerListener<Message> srvLsnr,
        IpcSharedMemoryServerEndpoint srv,
        TcpCommunicationMetricsListener metricsLsnr,
        IgniteLogger log,
        MessageFactory msgFactory,
        GridNioMessageWriterFactory writerFactory,
        GridNioMessageReaderFactory readerFactory,
        Tracing tracing
    ) {
        super(igniteInstanceName, WORKER_NAME, log);

        this.msgFactory = msgFactory;
        this.writerFactory = writerFactory;
        this.readerFactory = readerFactory;
        this.tracing = tracing;
        this.srv = srv;
        this.srvLsnr = srvLsnr;
        this.metricsLsnr = metricsLsnr;
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        try {
            while (!Thread.interrupted()) {
                IpcEndpoint ipcEndpoint = srv.accept();

                ShmemWorker e = new ShmemWorker(
                    igniteInstanceName(),
                    log,
                    tracing,
                    ipcEndpoint,
                    srvLsnr,
                    metricsLsnr,
                    readerFactory,
                    writerFactory,
                    msgFactory
                );

                e.onFinish(() -> shmemWorkers.remove(e));

                shmemWorkers.add(e);

                new IgniteThread(e).start();
            }
        }
        catch (IgniteCheckedException e) {
            if (!isCancelled())
                U.error(log, "Shmem server failed.", e);
        }
        finally {
            srv.close();
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        super.cancel();

        srv.close();
    }
}
