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

package org.apache.ignite.internal.processors.cache.persistence.cdc;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

/** */
public class CdcWorker extends GridWorker {
    /** Default throttling timeout in millis for polling CDC buffer. */
    public static final int DFLT_POLL_CDC_BUF_THROTTLING_TIMEOUT = 100;

    /** Throttling timeout in millis for polling CDC buffer. */
    private final long cdcBufPollTimeout = Long.getLong(
        IgniteSystemProperties.IGNITE_THROTTLE_POLL_CDC_BUF, DFLT_POLL_CDC_BUF_THROTTLING_TIMEOUT);

    /** CDC buffer. */
    private final CdcBuffer cdcBuf;

    /** CDC consumer. */
    private final CdcBufferConsumer consumer;

    /** */
    public CdcWorker(
        GridCacheSharedContext<?, ?> cctx,
        IgniteLogger log,
        CdcBuffer cdcBuf,
        CdcBufferConsumer consumer
    ) {
        super(cctx.igniteInstanceName(),
            "cdc-worker%" + cctx.igniteInstanceName(),
            log,
            cctx.kernalContext().workersRegistry());

        this.cdcBuf = cdcBuf;
        this.consumer = consumer;
    }

    /** */
    @Override public void body() {
        while (!isCancelled()) {
            updateHeartbeat();

            ByteBuffer data = cdcBuf.poll();

            if (data == null) {
                LockSupport.parkNanos(cdcBufPollTimeout * 1_000_000);  // millis to nanos.

                continue;
            }

            if (log.isDebugEnabled())
                log.debug("Poll a data bucket from CDC buffer [len=" + (data.limit() - data.position()) + ']');

            // TODO: Consumer must not block this system thread.
            consumer.consume(data);
        }

        consumer.close();
    }

    /** {@inheritDoc} */
    @Override protected void cleanup() {
        cdcBuf.cleanIfOverflowed();
    }

    /** */
    public void restart() {
        isCancelled.set(false);

        new IgniteThread(this).start();
    }
}
