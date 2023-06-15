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
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/** CDC processor responsible for collecting data changes in realtime within Ignite node. */
public class CdcProcessor {
    /** Buffer to store collected data. */
    private final CdcBuffer cdcBuf;

    /** CDC worker. */
    private final CdcWorker worker;

    /** Ignite log. */
    private final IgniteLogger log;

    /** Whether CDC is enabled. Disables after {@link #cdcBuf} overflows. */
    private boolean enabled = true;

    /** */
    public CdcProcessor(GridCacheSharedContext<?, ?> cctx, IgniteLogger log, long maxCdcBufSize, CdcBufferConsumer consumer) {
        this.log = log;

        cdcBuf = new CdcBuffer(maxCdcBufSize);
        worker = new CdcWorker(cctx, log, cdcBuf, consumer);
    }

    /**
     * @param dataBuf Buffer that contains data to collect.
     * @param off Offset of the data to collect in the buffer.
     * @param len Length of the data to collect.
     */
    public void collect(ByteBuffer dataBuf, int off, int len) {
        if (!enabled)
            return;

        int oldPos = dataBuf.position();

        try {
            byte[] data = new byte[len];

            dataBuf.position(off);

            dataBuf.get(data, 0, len);

            if (log.isDebugEnabled())
                log.debug("Offerring a data bucket to the CDC buffer [len=" + len + ']');

            if (!cdcBuf.offer(data)) {
                enabled = false;

                log.warning("CDC buffer has overflowed. Stop realtime mode of CDC.");

                worker.cancel();
            }
        }
        finally {
            dataBuf.position(oldPos);
        }
    }

    /** Start CDC worker. */
    public void start() {
        worker.restart();
    }

    /** Shutdown CDC worker. */
    public void shutdown() throws IgniteInterruptedCheckedException {
        worker.cancel();

        U.join(worker);
    }
}
