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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.MarshalledRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferWalIterator;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactoryImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;
import static org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordSerializerFactory.LATEST_SERIALIZER_VERSION;

/** CDC processor responsible for collecting data changes in realtime within Ignite node. */
public class CdcProcessor {
    /** Serializer latest version to use. TODO: get from WAL manager? */
    private final int serializerVer =
        IgniteSystemProperties.getInteger(IGNITE_WAL_SERIALIZER_VERSION, LATEST_SERIALIZER_VERSION);

    /** Buffer to store collected data. */
    private final CdcBuffer cdcBuf;

    /** CDC worker. */
    private final CdcWorker worker;

    /** Ignite log. */
    private final IgniteLogger log;

    /** */
    private final RecordSerializer serializer;

    /** Whether CDC is enabled. Disables after {@link #cdcBuf} overflows. */
    private volatile boolean enabled;

    /** */
    private WALPointer lastWrittenPtr;

    /** */
    public CdcProcessor(GridCacheSharedContext<?, ?> cctx, IgniteLogger log) throws IgniteCheckedException {
        this.log = log;

        serializer = new RecordSerializerFactoryImpl(
            cctx, (recType, recPtr) -> recType == WALRecord.RecordType.DATA_RECORD_V2)
            .marshalledMode(true)
            .createSerializer(serializerVer);

        cdcBuf = new CdcBuffer(cctx.gridConfig().getDataStorageConfiguration().getMaxCdcBufferSize());
        worker = new CdcWorker(cctx, log, cdcBuf);
    }

    /**
     * @param dataBuf Buffer that contains data to collect.
     */
    public void collect(ByteBuffer dataBuf) {
        if (!enabled)
            return;

        try (WALIterator walIt = new ByteBufferWalIterator(dataBuf, serializer, lastWrittenPtr)) {
            while (walIt.hasNext()) {
                MarshalledRecord rec = (MarshalledRecord)walIt.next().getValue();

                if (log.isDebugEnabled())
                    log.debug("Offerring a data bucket to the CDC buffer [len=" + rec.buffer().limit() + ']');

                if (!cdcBuf.offer(rec.buffer())) {
                    enabled = false;

                    log.warning("CDC buffer has overflowed. Stop realtime mode of CDC.");

                    return;
                }
            }

            assert walIt.lastRead().isPresent();

            lastWrittenPtr = walIt.lastRead().get();
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * Enable realtime CDC.
     *
     * @param lastWrittenPtr Pointer to last written record, excluded from CDC.
     */
    public void enable(@Nullable WALPointer lastWrittenPtr) {
        if (enabled)
            return;

        this.lastWrittenPtr = lastWrittenPtr;

        enabled = true;
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
