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

package org.apache.ignite.spi.communication.tcp.internal.shmem;

import java.util.Set;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.tracing.Tracing;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.ipc.IpcEndpoint;
import org.apache.ignite.internal.util.ipc.IpcToNioAdapter;
import org.apache.ignite.internal.util.nio.GridConnectionBytesVerifyFilter;
import org.apache.ignite.internal.util.nio.GridDirectParser;
import org.apache.ignite.internal.util.nio.GridNioCodecFilter;
import org.apache.ignite.internal.util.nio.GridNioMessageReaderFactory;
import org.apache.ignite.internal.util.nio.GridNioMessageWriterFactory;
import org.apache.ignite.internal.util.nio.GridNioServerListener;
import org.apache.ignite.internal.util.nio.GridNioTracerFilter;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;

/**
 * Shmem grid worker.
 */
public class ShmemWorker extends GridWorker {
    /** Worker name. */
    private static final String WORKER_NAME = "shmem-worker";

    /** Finish listeners. */
    private final Set<Runnable> finishListeners = new GridConcurrentLinkedHashSet<>();

    /** Endpoint. */
    private final IpcEndpoint endpoint;

    /** Server listener. */
    private final GridNioServerListener<Message> srvLsnr;

    /** Statistics. */
    private final TcpCommunicationMetricsListener metricsLsnr;

    /** Reader factory. */
    private final GridNioMessageReaderFactory readerFactory;

    /** Writer factory. */
    private final GridNioMessageWriterFactory writerFactory;

    /** Message factory. */
    private final MessageFactory msgFactory;

    /** Logger. */
    private final IgniteLogger log;

    /** Tracing. */
    private Tracing tracing;

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     * @param tracing Tracing.
     * @param endpoint Endpoint.
     * @param srvLsnr Server listener.
     * @param metricsLsnr Metrics listener.
     * @param readerFactory Reader factory.
     * @param writerFactory Writer factory.
     * @param msgFactory Message factory.
     */
    public ShmemWorker(
        String igniteInstanceName,
        IgniteLogger log,
        Tracing tracing,
        IpcEndpoint endpoint,
        GridNioServerListener<Message> srvLsnr,
        TcpCommunicationMetricsListener metricsLsnr,
        GridNioMessageReaderFactory readerFactory,
        GridNioMessageWriterFactory writerFactory,
        MessageFactory msgFactory
    ) {
        super(igniteInstanceName, WORKER_NAME, log);

        this.endpoint = endpoint;
        this.log = log;
        this.tracing = tracing;
        this.srvLsnr = srvLsnr;
        this.metricsLsnr = metricsLsnr;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.msgFactory = msgFactory;
    }

    /** {@inheritDoc} */
    @Override protected void body() throws InterruptedException {
        try {

            IpcToNioAdapter<Message> adapter = new IpcToNioAdapter<>(
                metricsLsnr.metricRegistry(),
                log,
                endpoint,
                srvLsnr,
                writerFactory,
                new GridNioTracerFilter(log, tracing),
                new GridNioCodecFilter(
                    new GridDirectParser(log.getLogger(GridDirectParser.class), msgFactory, readerFactory),
                    log,
                    true),
                new GridConnectionBytesVerifyFilter(log)
            );

            adapter.serve();
        }
        finally {
            finishListeners.forEach(Runnable::run);

            endpoint.close();
        }
    }

    /**
     * @param lsnr Listener to onFinish event.
     */
    public void onFinish(Runnable lsnr) {
        finishListeners.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        super.cancel();

        endpoint.close();
    }

    /** {@inheritDoc} */
    @Override protected void cleanup() {
        super.cleanup();

        endpoint.close();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ShmemWorker.class, this);
    }
}
