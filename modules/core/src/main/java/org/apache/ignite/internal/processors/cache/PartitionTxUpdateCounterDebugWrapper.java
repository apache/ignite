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

    /** */
    private CacheGroupContext grp;

    /**
     * @param grp Group.
     * @param partId Part id.
     */
    public PartitionTxUpdateCounterDebugWrapper(CacheGroupContext grp, int partId) {
        this.log = grp.shared().logger(getClass());
        this.partId = partId;
        this.grp = grp;
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        super.init(initUpdCntr, cntrUpdData);

        log.info("[op=init" +
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
            log.info(sb.a(", after=" + toString() +
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
            log.info(sb.a(", after=" + toString() +
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
            log.info(sb.a(", after=" + toString() +
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
            log.info(sb.a(", after=" + toString() +
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
            log.info(sb.a(", after=" + toString() +
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
            return super.reserve(delta);
        }
        finally {
            log.info(sb.a(", after=" + toString() +
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
            log.info(sb.a(", after=" + toString() +
                ']').toString());
        }

        return updated;
    }
}
