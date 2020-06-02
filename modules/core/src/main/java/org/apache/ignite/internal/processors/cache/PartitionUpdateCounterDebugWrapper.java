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
import org.jetbrains.annotations.Nullable;

/**
 * Update counter wrapper with logging capabilities.
 */
public class PartitionUpdateCounterDebugWrapper implements PartitionUpdateCounter {
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
    public PartitionUpdateCounterDebugWrapper(int partId, PartitionUpdateCounter delegate) {
        this.partId = partId;
        this.grp = delegate.context();
        this.log = grp.shared().logger(getClass());
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        delegate.init(initUpdCntr, cntrUpdData);

        log.debug("[op=init" +
            ", grpId=" + grp.groupId() +
            ", grpName=" + grp.cacheOrGroupName() +
            ", caches=" + grp.caches() +
            ", atomicity=" + grp.config().getAtomicityMode() +
            ", syncMode=" + grp.config().getWriteSynchronizationMode() +
            ", mode=" + grp.config().getCacheMode() +
            ", partId=" + partId +
            ", gapsLen=" + (cntrUpdData != null ? cntrUpdData.length : 0) +
            ", cur=" + toString() +
            ']');
    }

    /** {@inheritDoc} */
    @Override public void updateInitial(long start, long delta) {
        SB sb = new SB();

        sb.a("[op=updateInitial" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", range=(" + start + "," + delta + ")" +
            ", before=" + toString());

        try {
            delegate.updateInitial(start, delta);
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public long next() {
        SB sb = new SB();

        sb.a("[op=next" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", before=" + toString());

        try {
            return delegate.next();
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public long next(long delta) {
        SB sb = new SB();

        sb.a("[op=next" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", delta=" + delta +
            ", before=" + toString());

        try {
            return delegate.next();
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized void update(long val) throws IgniteCheckedException {
        SB sb = new SB();

        sb.a("[op=set" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", val=" + val +
            ", before=" + toString());

        try {
            delegate.update(val);
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized GridLongList finalizeUpdateCounters() {
        SB sb = new SB();

        sb.a("[op=finalizeUpdateCounters" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", before=" + toString() +
            ']');

        try {
            return delegate.finalizeUpdateCounters();
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized long reserve(long delta) {
        SB sb = new SB();

        sb.a("[op=reserve" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", delta=" + delta +
            ", before=" + toString());

        try {
            return delegate.reserve(delta);
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean update(long start, long delta) {
        SB sb = new SB();

        sb.a("[op=update" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", delta=(" + start + "," + delta + ")" +
            ", before=" + toString());

        boolean updated = false;

        try {
            updated = delegate.update(start, delta);
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }

        return updated;
    }

    /** {@inheritDoc} */
    @Override public synchronized void reset() {
        SB sb = new SB();

        sb.a("[op=reset" +
            ", grpId=" + grp.groupId() +
            ", partId=" + partId +
            ", before=" + toString());

        try {
            delegate.reset();
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }
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
    @Override public long reserved() {
        return delegate.reserved();
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
        return false;
    }

    /** {@inheritDoc} */
    @Override public Iterator<long[]> iterator() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public CacheGroupContext context() {
        return delegate.context();
    }

    /** {@inheritDoc} */
    @Override public PartitionUpdateCounter copy() {
        return new PartitionUpdateCounterDebugWrapper(partId, delegate.copy());
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return delegate.toString();
    }
}
