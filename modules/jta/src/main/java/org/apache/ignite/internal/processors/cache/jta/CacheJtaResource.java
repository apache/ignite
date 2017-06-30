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

package org.apache.ignite.internal.processors.cache.jta;

import java.util.concurrent.atomic.AtomicReference;
import javax.cache.CacheException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.transactions.TransactionState;

import static org.apache.ignite.transactions.TransactionState.ACTIVE;
import static org.apache.ignite.transactions.TransactionState.COMMITTED;
import static org.apache.ignite.transactions.TransactionState.PREPARED;
import static org.apache.ignite.transactions.TransactionState.ROLLED_BACK;

/**
 * Cache {@link XAResource} and {@link Synchronization} implementation.
 */
final class CacheJtaResource implements XAResource, Synchronization {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** */
    private static IgniteLogger log;

    /** */
    private static final Xid[] NO_XID = new Xid[] {};

    /** Cache transaction. */
    private GridNearTxLocal cacheTx;

    /** */
    private Xid xid;

    /**
     * @param cacheTx Cache jta.
     * @param ctx Kernal context.
     */
    CacheJtaResource(GridNearTxLocal cacheTx, GridKernalContext ctx) {
        assert cacheTx != null;
        assert ctx != null;

        this.cacheTx = cacheTx;

        if (log == null)
            log = U.logger(ctx, logRef, CacheJtaResource.class);
    }

    /** {@inheritDoc} */
    @Override public void start(Xid xid, int flags) throws XAException {
        if (log.isDebugEnabled())
            log.debug("XA resource start(...) [xid=" + xid + ", flags=<" + flags(flags) + ">]");

        // Simply save global transaction id.
        this.xid = xid;

        if ((flags & TMRESUME) == TMRESUME)
            try {
                cacheTx.resume();
            }
            catch (IgniteCheckedException e) {
                throwException("Failed to resume transaction.", e);
            }
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
    @Override public void end(Xid xid, int flags) throws XAException {
        assert this.xid.equals(xid);

        if (log.isDebugEnabled())
            log.debug("XA resource end(...) [xid=" + xid + ", flags=<" + flags(flags) + ">]");

        if ((flags & TMSUSPEND) == TMSUSPEND)
            try {
                cacheTx.suspend();
            }
            catch (IgniteCheckedException e) {
                throwException("Failed to suspend transaction.", e);
            }
        else if ((flags & TMFAIL) > 0)
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
        return (int)(cacheTx.timeout() / 1000);
    }

    /** {@inheritDoc} */
    @Override public boolean setTransactionTimeout(int i) {
        cacheTx.timeout(i * 1000);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isSameRM(XAResource xar) {
        if (xar == this)
            return true;

        if (!(xar instanceof CacheJtaResource))
            return false;

        CacheJtaResource other = (CacheJtaResource)xar;

        return cacheTx == other.cacheTx;
    }

    /** {@inheritDoc} */
    @Override public void beforeCompletion() {
        if (log.isDebugEnabled())
            log.debug("Synchronization.beforeCompletion() [xid=" + cacheTx.xid() + "]");

        if (cacheTx.state() != ACTIVE)
            throw new CacheException("Cache transaction is not in active state.");

        try {
            cacheTx.prepare();
        }
        catch (IgniteCheckedException e) {
            throw new CacheException("Failed to prepare cache transaction.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void afterCompletion(int status) {
        switch (status) {
            case Status.STATUS_COMMITTED:
                if (log.isDebugEnabled())
                    log.debug("Synchronization.afterCompletion(STATUS_COMMITTED) [xid=" + cacheTx.xid() + "]");

                try {
                    cacheTx.commit();
                }
                catch (IgniteCheckedException e) {
                    throw new CacheException("Failed to commit cache transaction.", e);
                }

                break;

            case Status.STATUS_ROLLEDBACK:
                if (log.isDebugEnabled())
                    log.debug("Synchronization.afterCompletion(STATUS_ROLLEDBACK) [xid=" + cacheTx.xid() + "]");

                try {
                    cacheTx.rollback();
                }
                catch (IgniteCheckedException e) {
                    throw new CacheException("Failed to rollback cache transaction.", e);
                }

                break;

            default:
                throw new IllegalArgumentException("Unknown transaction status: " + status);
        }
    }

    /**
     *
     * @return {@code true} if jta was already committed or rolled back.
     */
    boolean isFinished() {
        TransactionState state = cacheTx.state();

        return state == COMMITTED || state == ROLLED_BACK;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheJtaResource.class, this);
    }
}
