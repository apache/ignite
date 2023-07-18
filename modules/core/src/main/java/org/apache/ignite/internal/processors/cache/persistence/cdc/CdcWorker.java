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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cdc.CdcEvent;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.cdc.CdcEventImpl;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.RealtimeCdcRecord;
import org.apache.ignite.internal.pagemem.wal.record.RealtimeCdcStopRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferBackedDataInputImpl;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordDataV2Serializer;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

import static org.apache.ignite.failure.FailureType.CRITICAL_ERROR;

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
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final RecordDataV2Serializer dataRecSer;

    /** */
    public CdcWorker(GridCacheSharedContext<?, ?> cctx, IgniteLogger log, CdcBuffer cdcBuf) {
        super(cctx.igniteInstanceName(),
            "cdc-worker%" + cctx.igniteInstanceName(),
            log,
            cctx.kernalContext().workersRegistry());

        this.cctx = cctx;
        this.cdcBuf = cdcBuf;

        consumer = cctx.gridConfig().getDataStorageConfiguration().getCdcConsumer();

        // TODO: get version from WAL manager?
        dataRecSer = new RecordDataV2Serializer(cctx);
    }

    /** */
    @Override public void body() {
        // TODO: concurrent stop? do I need acquire a lock here?
        if (cctx.kernalContext().isStopping())
            return;

        try {
            while (!isCancelled()) {
                updateHeartbeat();

                if (cdcBuf.overflowed()) {
                    log(new RealtimeCdcStopRecord());

                    cancel();

                    return;
                }

                ByteBuffer data = cdcBuf.poll();

                if (data == null) {
                    LockSupport.parkNanos(cdcBufPollTimeout * 1_000_000);  // millis to nanos.

                    continue;
                }

                if (log.isDebugEnabled())
                    log.debug("Poll a data bucket from CDC buffer [len=" + (data.limit() - data.position()) + ']');

                // TODO: Consumer must not block this system thread. Or this thread should not be system thread?
                if (consumer.consume(cdcEvents(data)))
                    log(new RealtimeCdcRecord());
            }
        }
        catch (Exception e) {
            cctx.kernalContext().failure().process(new FailureContext(CRITICAL_ERROR, e));
        }
    }

    /** */
    private Collection<CdcEvent> cdcEvents(ByteBuffer buf) throws IgniteCheckedException, IOException {
        List<CdcEvent> cdcEvts = new ArrayList<>();

        while (buf.hasRemaining()) {
            int recPos = buf.position();

            // -1 is a magic number here. See RecordV2Serializer#recordIO#readWithHeaders, marshaller mode.
            WALRecord.RecordType type = WALRecord.RecordType.fromIndex(buf.get() - 1);

            assert type == WALRecord.RecordType.DATA_RECORD_V2 : type;

            buf.position(recPos + 1 + 8 + 4);  // type + walSegIdx + fileOff.
            int size = buf.getInt();

            DataRecord dataRec = (DataRecord)dataRecSer.readRecord(type, new ByteBufferBackedDataInputImpl().buffer(buf), size);

            for (DataEntry e: dataRec.writeEntries()) {
                CacheObjectValueContext coctx = cctx.cacheObjectContext(e.cacheId());

                cdcEvts.add(new CdcEventImpl(
                    e.key().value(coctx, false),
                    e.value().valueBytes(coctx),
                    (e.flags() & DataEntry.PRIMARY_FLAG) != 0,
                    e.partitionId(),
                    e.writeVersion(),
                    e.cacheId(),
                    e.expireTime()
                ));
            }

            buf.position(buf.position() + 4);  // Skip CRC.
        }

        return cdcEvts;
    }

    /** */
    // TODO: rethink after IGNITE-19637. NULL might return during node start up, then overflowing was during memory restore.
    //       What to do in such case?
    private void log(WALRecord rec) throws IgniteCheckedException {
        if (cctx.wal().log(rec) == null) {
            long maxCdcBufSize = cctx.gridConfig().getDataStorageConfiguration().getMaxCdcBufferSize();

            log.error("Realtime CDC failed writing WAL record. CDC buffer size might be too low" +
                " [rec=" + rec + ", maxCdcBufSize=" + maxCdcBufSize + ']');
        }
    }

    /** {@inheritDoc} */
    @Override protected void cleanup() {
        consumer.close();

        cdcBuf.clean();
    }

    /** */
    public void restart() {
        isCancelled.set(false);

        new IgniteThread(this).start();
    }
}
