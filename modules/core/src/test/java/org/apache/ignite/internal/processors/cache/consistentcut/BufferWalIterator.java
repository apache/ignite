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

package org.apache.ignite.internal.processors.cache.consistentcut;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.wal.WALPointer;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Iterator wrapper over WALIterator {@link #walIt} that stores read records in buffer to re-read them later.
 *
 * Consistent Cut algorithm requires to re-read some parts of WAL multiple times, then usage of the buffer is cheaper
 * than frequent use of {@link IgniteWriteAheadLogManager#replay(WALPointer)}.
 */
class BufferWalIterator extends GridIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>> implements WALIterator {
    /** Buffer to store read items from {@link #walIt} to re-read them at future. */
    private List<IgniteBiTuple<WALPointer, WALRecord>> buf;

    /** Iterator over copy of buffer at moment of iterator creation. */
    private Iterator<IgniteBiTuple<WALPointer, WALRecord>> bufIt;

    /** Underlying WAL iterator. */
    private final WALIterator walIt;

    /** Buffer Mode: CLEAN or STORE. */
    private BufferedMode mode;

    /** Current record to read from the buffer. */
    private IgniteBiTuple<WALPointer, WALRecord> curBufItem;

    /** Collection of transactions to skip while read them from the buffer. */
    private Set<IgniteUuid> skipTx;

    /** */
    BufferWalIterator(WALIterator walIt) {
        this.walIt = walIt;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        return bufHasNext() || walIt.hasNext();
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple<WALPointer, WALRecord> nextX() throws IgniteCheckedException {
        // Returns item from the buffer.
        if (curBufItem != null) {
            IgniteBiTuple<WALPointer, WALRecord> next = curBufItem;

            curBufItem = null;

            return next;
        }

        IgniteBiTuple<WALPointer, WALRecord> rec = walIt.next();

        if (mode == BufferedMode.STORE)
            buf.add(rec);

        return rec;
    }

    /**
     * @return {@code true} if buffer has next item, otherwise {@code false}.
     */
    private boolean bufHasNext() {
        if (bufIt == null)
            return false;

        while (bufIt.hasNext()) {
            curBufItem = bufIt.next();

            if (mode == BufferedMode.CLEAN)
                buf.remove(0);

            if (skipReadRecordFromBuffer(curBufItem.getValue()))
                continue;

            break;
        }

        if (curBufItem == null)
            cleanBuf();

        return curBufItem != null;
    }

    /** */
    private void cleanBuf() {
        bufIt = null;

        if (mode == BufferedMode.CLEAN)
            buf = null;
    }

    /** */
    void mode(BufferedMode mode) {
        this.mode = mode;

        if (mode == BufferedMode.STORE && buf == null)
            buf = new ArrayList<>();
    }

    /** Reset buffer before read from it. */
    void resetBuffer() {
        if (buf != null)
            bufIt = new ArrayList<>(buf).iterator();
    }

    /** */
    void skipTxInBuffer(Set<IgniteUuid> skipTx) {
        if (this.skipTx != null)
            this.skipTx.addAll(skipTx);
        else
            this.skipTx = new HashSet<>(skipTx);
    }

    /** */
    List<IgniteBiTuple<WALPointer, WALRecord>> buffer() {
        return buf;
    }

    /** */
    Set<IgniteUuid> skipTx() {
        return skipTx;
    }

    /** {@inheritDoc} */
    @Override public void close() throws IgniteCheckedException {
        walIt.close();

        buf = null;
        bufIt = null;
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return !bufIt.hasNext() && walIt.isClosed();
    }

    /** */
    private boolean skipReadRecordFromBuffer(WALRecord rec) {
        if (rec instanceof DataRecord && skipDataRecord((DataRecord)rec))
            return true;

        if (rec instanceof TxRecord && skipTxRecord((TxRecord)rec))
            return true;

        return false;
    }

    /** */
    private boolean skipDataRecord(DataRecord r) {
        IgniteUuid tx = r.writeEntries().get(0).nearXidVersion().asIgniteUuid();

        return skipTx != null && skipTx.contains(tx);
    }

    /** */
    private boolean skipTxRecord(TxRecord r) {
        return skipTx != null && skipTx.contains(r.nearXidVersion().asIgniteUuid());
    }

    /** {@inheritDoc} */
    @Override public Optional<WALPointer> lastRead() {
        assert false : "Should not be invoked";

        return Optional.empty();
    }

    /** {@inheritDoc} */
    @Override public void removeX() {
        assert false : "Should not be invoked";
    }

    /** Buffer modes. */
    enum BufferedMode {
        /** Do not store new read records from WAL. */
        CLEAN,

        /** Store records new read records from WAL to internal buffer. */
        STORE
    }
}
