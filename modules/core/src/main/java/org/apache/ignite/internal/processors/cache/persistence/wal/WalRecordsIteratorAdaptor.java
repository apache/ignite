/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import java.util.Optional;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.util.GridCloseableIteratorAdapter;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.NotNull;

/**
 * Iterator over WAL segments. This abstract class provides most functionality for reading records.
 */
public abstract class WalRecordsIteratorAdaptor
    extends GridCloseableIteratorAdapter<IgniteBiTuple<WALPointer, WALRecord>> implements WALIterator {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Current record preloaded, to be returned on next()<br> Normally this should be not null because advance() method
     * should already prepare some value<br>
     */
    protected IgniteBiTuple<WALPointer, WALRecord> curRec;

    /**
     * The exception which can be thrown during reading next record. It holds until the next calling of next record.
     */
    private IgniteCheckedException curException;

    /** Logger */
    @NotNull protected final IgniteLogger log;

    /** Position of last read valid record. */
    private WALPointer lastRead;

    /**
     * @param log Logger.
     */
    protected WalRecordsIteratorAdaptor(@NotNull final IgniteLogger log) {
        this.log = log;
    }

    /** {@inheritDoc} */
    @Override protected IgniteBiTuple<WALPointer, WALRecord> onNext() throws IgniteCheckedException {
        if (curException != null)
            throw curException;

        IgniteBiTuple<WALPointer, WALRecord> ret = curRec;

        try {
            advance();
        }
        catch (IgniteCheckedException e) {
            curException = e;
        }

        return ret;
    }

    /** {@inheritDoc} */
    @Override protected boolean onHasNext() throws IgniteCheckedException {
        if (curException != null)
            throw curException;

        return curRec != null;
    }

    /**
     * Switches records iterator to the next record. <ul> <li>{@link #curRec} will be updated.</li> </ul>
     *
     * {@code advance()} runs a step ahead {@link #next()}
     *
     * @throws IgniteCheckedException If failed.
     */
    protected abstract void advance() throws IgniteCheckedException;

    /** {@inheritDoc} */
    @Override public Optional<WALPointer> lastRead() {
        return Optional.ofNullable(lastRead);
    }

    protected void lastRead(WALPointer lastRead) {
        this.lastRead = lastRead;
    }
}
