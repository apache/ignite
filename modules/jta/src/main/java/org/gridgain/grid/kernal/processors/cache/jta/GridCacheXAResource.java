/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.jta;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.transaction.xa.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.transactions.GridCacheTxState.*;

/**
 * Cache XA resource implementation.
 */
public final class GridCacheXAResource implements XAResource {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** */
    private static final Xid[] NO_XID = new Xid[] {};

    /** Context. */
    private GridCacheContext cctx;

    /** Cache transaction. */
    private GridCacheTxEx cacheTx;

    /** */
    private IgniteLogger log;

    /** */
    private Xid xid;

    /**
     * @param cacheTx Cache jta.
     * @param cctx Cache context.
     */
    public GridCacheXAResource(GridCacheTxEx cacheTx, GridCacheContext cctx) {
        assert cacheTx != null;
        assert cctx != null;

        this.cctx = cctx;
        this.cacheTx = cacheTx;

        log = U.logger(cctx.kernalContext(), logRef, GridCacheXAResource.class);
    }

    /** {@inheritDoc} */
    @Override public void start(Xid xid, int flags) {
        if (log.isDebugEnabled())
            log.debug("XA resource start(...) [xid=" + xid + ", flags=<" + flags(flags) + ">]");

        // Simply save global transaction id.
        this.xid = xid;
    }

    /**
     * @param msg Message.
     * @param cause Cause.
     * @throws XAException XA exception.
     */
    private void throwException(String msg, Throwable cause) throws XAException {
        XAException ex = new XAException(msg);

        ex.initCause(cause);

        throw ex;
    }

    /** {@inheritDoc} */
    @Override public void rollback(Xid xid) throws XAException {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
            log.debug("XA resource rollback(...) [xid=" + xid + "]");

        try {
            cacheTx.rollback();
        }
        catch (IgniteCheckedException e) {
            throwException("Failed to rollback cache transaction: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public int prepare(Xid xid) throws XAException {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
            log.debug("XA resource prepare(...) [xid=" + xid + "]");

        if (cacheTx.state() != ACTIVE)
            throw new XAException("Cache transaction is not in active state.");

        try {
            cacheTx.prepare();
        }
        catch (IgniteCheckedException e) {
            throwException("Failed to prepare cache transaction.", e);
        }

        return XA_OK;
    }

    /** {@inheritDoc} */
    @Override public void end(Xid xid, int flags) {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
            log.debug("XA resource end(...) [xid=" + xid + ", flags=<" + flags(flags) + ">]");

        if ((flags & TMFAIL) > 0)
            cacheTx.setRollbackOnly();
    }

    /** {@inheritDoc} */
    @Override public void commit(Xid xid, boolean onePhase) throws XAException {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
            log.debug("XA resource commit(...) [xid=" + xid + ", onePhase=" + onePhase + "]");

        try {
            cacheTx.commit();
        }
        catch (IgniteCheckedException e) {
            throwException("Failed to commit cache transaction: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public void forget(Xid xid) throws XAException {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
            log.debug("XA resource forget(...) [xid=" + xid + "]");

        try {
            cacheTx.invalidate(true);

            cacheTx.commit();
        }
        catch (IgniteCheckedException e) {
            throwException("Failed to forget cache transaction: " + e.getMessage(), e);
        }
    }

    /** {@inheritDoc} */
    @Override public Xid[] recover(int i) {
        if (cacheTx.state() == PREPARED)
            return new Xid[] { xid };

        return NO_XID;
    }

    /**
     * @param flags JTA Flags.
     * @return Comma-separated flags string.
     */
    private String flags(int flags) {
        StringBuilder res = new StringBuilder();

        addFlag(res, flags, TMENDRSCAN, "TMENDRSCAN");
        addFlag(res, flags, TMFAIL, "TMFAIL");
        addFlag(res, flags, TMJOIN, "TMJOIN");
        addFlag(res, flags, TMNOFLAGS, "TMNOFLAGS");
        addFlag(res, flags, TMONEPHASE, "TMONEPHASE");
        addFlag(res, flags, TMRESUME, "TMRESUME");
        addFlag(res, flags, TMSTARTRSCAN, "TMSTARTRSCAN");
        addFlag(res, flags, TMSUCCESS, "TMSUCCESS");
        addFlag(res, flags, TMSUSPEND, "TMSUSPEND");

        return res.toString();
    }

    /**
     * @param sb String builder.
     * @param flags Flags bit set.
     * @param mask Bit mask.
     * @param flagName String name of the flag specified by given mask.
     * @return String builder appended by flag if it's presented in bit set.
     */
    private StringBuilder addFlag(StringBuilder sb, int flags, int mask, String flagName) {
        if ((flags & mask) > 0)
            sb.append(sb.length() > 0 ? "," : "").append(flagName);

        return sb;
    }

    /** {@inheritDoc} */
    @Override public int getTransactionTimeout() {
        return (int)cacheTx.timeout();
    }

    /** {@inheritDoc} */
    @Override public boolean isSameRM(XAResource xar) {
        if (xar == this)
            return true;

        if  (!(xar instanceof GridCacheXAResource))
            return false;

        GridCacheXAResource other = (GridCacheXAResource)xar;

        return cctx == other.cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean setTransactionTimeout(int i) {
        cacheTx.timeout(i);

        return true;
    }

    /**
     *
     * @return {@code true} if jta was already committed or rolled back.
     */
    public boolean isFinished() {
        GridCacheTxState state = cacheTx.state();

        return state == COMMITTED || state == ROLLED_BACK;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheXAResource.class, this);
    }
}
