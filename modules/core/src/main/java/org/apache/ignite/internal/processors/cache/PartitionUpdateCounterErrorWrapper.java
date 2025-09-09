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

package org.apache.ignite.internal.processors.cache;

import java.util.Iterator;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

/**
 * Update counter wrapper for error logging.
 */
public class PartitionUpdateCounterErrorWrapper implements PartitionUpdateCounter {
    /** */
    private final IgniteLogger log;

    /** */
    private final int partId;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final PartitionUpdateCounter delegate;

    /**
     * @param partId Part id.
     * @param delegate Delegate.
     */
    public PartitionUpdateCounterErrorWrapper(int partId, PartitionUpdateCounter delegate) {
        this.partId = partId;
        this.grp = delegate.context();
        this.log = grp.shared().logger(getClass());
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        try {
            return delegate.reserve(delta);
        }
        catch (AssertionError e) {
            SB sb = new SB();

            sb.a("Failed to increment HWM ")
                .a("[op=reserve")
                .a(", grpId=").a(grp.groupId())
                .a(", grpName=").a(grp.cacheOrGroupName())
                .a(", caches=").a(grp.caches())
                .a(", atomicity=").a(grp.config().getAtomicityMode())
                .a(", syncMode=").a(grp.config().getWriteSynchronizationMode())
                .a(", mode=").a(grp.config().getCacheMode())
                .a(", partId=").a(partId)
                .a(", delta=").a(delta)
                .a("]");

            U.error(log, sb.toString());

            throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        delegate.init(initUpdCntr, cntrUpdData);
    }

    /** {@inheritDoc} */
    @Override public long next() {
        return delegate.next();
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        return delegate.next(delta);
    }

    /** {@inheritDoc} */
    @Override public void update(long val) throws IgniteCheckedException {
        delegate.update(val);
    }

    /** {@inheritDoc} */
    @Override public boolean update(long start, long delta) {
        return delegate.update(start, delta);
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        delegate.reset();
    }

    /** {@inheritDoc} */
    @Override public void updateInitial(long start, long delta) {
        delegate.updateInitial(start, delta);
    }

    /** {@inheritDoc} */
    @Override public GridLongList finalizeUpdateCounters() {
        return delegate.finalizeUpdateCounters();
    }

    /** {@inheritDoc} */
    @Override public void resetInitialCounter() {
        delegate.resetInitialCounter();
    }

    /** {@inheritDoc} */
    @Override public long initial() {
        return delegate.initial();
    }

    /** {@inheritDoc} */
    @Override public long get() {
        return delegate.get();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (o instanceof PartitionUpdateCounterErrorWrapper)
            return delegate.equals(((PartitionUpdateCounterErrorWrapper)o).delegate);

        return false;
    }

    /** {@inheritDoc} */
    @Override public long reserved() {
        return delegate.reserved();
    }

    /** {@inheritDoc} */
    @Override public long highestAppliedCounter() {
        return delegate.highestAppliedCounter();
    }

    /** {@inheritDoc} */
    @Nullable @Override public byte[] getBytes() {
        return delegate.getBytes();
    }

    /** {@inheritDoc} */
    @Override public boolean sequential() {
        return delegate.sequential();
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return delegate.empty();
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return delegate.iterator();
    }

    /** {@inheritDoc} */
    @Override public CacheGroupContext context() {
        return delegate.context();
    }

    /** {@inheritDoc} */
    @Override public PartitionUpdateCounter copy() {
        return new PartitionUpdateCounterErrorWrapper(partId, delegate.copy());
    }

    /** {@inheritDoc} */
    @Override public Object comparableState() {
        return delegate.comparableState();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}
