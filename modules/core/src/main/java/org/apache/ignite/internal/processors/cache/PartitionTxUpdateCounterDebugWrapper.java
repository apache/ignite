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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

/**
 * Update counter implementation useful for debugging.
 */
public class PartitionTxUpdateCounterDebugWrapper extends PartitionTxUpdateCounterImpl {
    /** */
    private IgniteLogger log;

    /** */
    private int partId;

    /**
     * @param grp Group.
     * @param partId Part id.
     */
    public PartitionTxUpdateCounterDebugWrapper(CacheGroupContext grp, int partId) {
        super(grp);
        this.log = grp.shared().logger(getClass());
        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        super.init(initUpdCntr, cntrUpdData);

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
            super.updateInitial(start, delta);
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
            return super.next();
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
            return super.next();
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
            super.update(val);
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
            return super.finalizeUpdateCounters();
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
            long cntr = get();

            long reserved = reserveCntr.getAndAdd(delta);

            assert reserved >= cntr : "LWM after HWM: lwm=" + cntr + ", hwm=" + reserved + ", grpId=" + grp.groupId()
                + ", partId=" + partId + ", cntr=" + super.toString();

            return reserved;
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
            updated = super.update(start, delta);
        }
        finally {
            log.debug(sb.a(", after=" + toString() +
                ']').toString());
        }

        return updated;
    }
}
