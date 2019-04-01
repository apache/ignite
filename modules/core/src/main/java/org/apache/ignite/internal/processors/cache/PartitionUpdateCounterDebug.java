package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class PartitionUpdateCounterDebug extends PartitionUpdateCounterImpl {
    /** */
    private IgniteLogger log;

    /** */
    private int partId;

    /** */
    private CacheGroupContext grp;

    /** */
    private int filterGrpId;

    /**
     * @param grp Group.
     * @param partId Part id.
     * @param filterGrpId Filter group id.
     */
    public PartitionUpdateCounterDebug(CacheGroupContext grp, int partId, int filterGrpId) {
        this.log = grp.shared().logger(getClass());
        this.partId = partId;
        this.grp = grp;
        this.filterGrpId = filterGrpId;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] rawGapsData) {
        super.init(initUpdCntr, rawGapsData);

        if (logIfNeeded())
            log.info("[op=init" +
                ", grpId=" + grp.groupId() +
                ", grpName=" + grp.cacheOrGroupName() +
                ", caches=" + grp.caches() +
                ", atomicity=" + grp.config().getAtomicityMode() +
                ", syncMode=" + grp.config().getWriteSynchronizationMode() +
                ", mode=" + grp.config().getCacheMode() +
                ", partId=" + partId +
                ", gapsLen=" + (rawGapsData != null ? rawGapsData.length : 0) +
                ", cur=" + toString() +
                ']');
    }

    /** {@inheritDoc} */
    @Override public synchronized void update(long val) throws IgniteCheckedException {
        SB sb = new SB();

        if (logIfNeeded())
            sb.a("[op=set" +
                ", grpId=" + grp.groupId() +
                ", partId=" + partId +
                ", val=" + val +
                ", before=" + toString());

        try {
            super.update(val);
        }
        finally {
            if (logIfNeeded())
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized GridLongList finalizeUpdateCounters() {
        SB sb = new SB();

        if (logIfNeeded())
            sb.a("[op=finalizeUpdateCounters" +
                ", grpId=" + grp.groupId() +
                ", partId=" + partId +
                ", before=" + toString() +
                ']');

        try {
            return super.finalizeUpdateCounters();
        }
        finally {
            if (logIfNeeded())
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public long reserve(long delta) {
        SB sb = new SB();

        if (logIfNeeded())
            sb.a("[op=reserve" +
                ", grpId=" + grp.groupId() +
                ", partId=" + partId +
                ", delta=" + delta +
                ", before=" + toString());

        try {
            long cntr = get();

            long reserved = super.reserve(delta);

            assert reserved >= cntr : "Update counter behind reserve counter: cntr=" + cntr + ", reserveCntr=" + reserved;

            return reserved;
        }
        finally {
            if (logIfNeeded())
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean update(long start, long delta) {
        SB sb = new SB();

        if (logIfNeeded())
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
            if (logIfNeeded())
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }

        return updated;
    }

    /**
     * @return {@code True} if logging is needed.
     */
    private boolean logIfNeeded() {
        return filterGrpId == 0 || filterGrpId == grp.groupId();
    }
}
