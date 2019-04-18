package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

/**
 * TODO FIXME auto-enable with debug logging.
 */
public class PartitionUpdateCounterDebugWrapper extends PartitionUpdateCounterImpl {
    /** */
    private IgniteLogger log;

    /** */
    private int partId;

    /** */
    private CacheGroupContext grp;

    /** */
    private boolean doLogging;

    /**
     * @param grp Group.
     * @param partId Part id.
     * @param filterGrpId Filter group id.
     */
    public PartitionUpdateCounterDebugWrapper(CacheGroupContext grp, int partId, int filterGrpId) {
        this.log = grp.shared().logger(getClass());
        this.partId = partId;
        this.grp = grp;
        this.doLogging = filterGrpId == 0 || filterGrpId == grp.groupId();
    }

    /** {@inheritDoc} */
    @Override public void init(long initUpdCntr, @Nullable byte[] cntrUpdData) {
        super.init(initUpdCntr, cntrUpdData);

        if (doLogging)
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

    /** {@inheritDoc} */
    @Override public synchronized void update(long val) throws IgniteCheckedException {
        SB sb = new SB();

        if (doLogging)
            sb.a("[op=set" +
                ", grpId=" + grp.groupId() +
                ", partId=" + partId +
                ", val=" + val +
                ", before=" + toString());

        try {
            super.update(val);
        }
        finally {
            if (doLogging)
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized GridLongList finalizeUpdateCounters() {
        SB sb = new SB();

        if (doLogging)
            sb.a("[op=finalizeUpdateCounters" +
                ", grpId=" + grp.groupId() +
                ", partId=" + partId +
                ", before=" + toString() +
                ']');

        try {
            return super.finalizeUpdateCounters();
        }
        finally {
            if (doLogging)
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized long reserve(long delta) {
        SB sb = new SB();

        if (doLogging)
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
            if (doLogging)
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean update(long start, long delta) {
        SB sb = new SB();

        if (doLogging)
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
            if (doLogging)
                log.info(sb.a(", after=" + toString() +
                    ']').toString());
        }

        return updated;
    }
}
