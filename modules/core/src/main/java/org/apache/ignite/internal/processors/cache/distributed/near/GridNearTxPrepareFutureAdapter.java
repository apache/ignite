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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheFuture;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxMapping;
import org.apache.ignite.internal.processors.cache.distributed.dht.colocated.GridDhtDetachedCacheEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxKey;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.future.GridCompoundIdentityFuture;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteUuid;

import static org.apache.ignite.internal.processors.cache.GridCacheOperation.NOOP;

/**
 * Common code for tx prepare in optimistic and pessimistic modes.
 */
public abstract class GridNearTxPrepareFutureAdapter extends GridCompoundIdentityFuture<IgniteInternalTx>
    implements GridCacheFuture<IgniteInternalTx> {
    /** Logger reference. */
    protected static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** */
    private static final IgniteReducer<IgniteInternalTx, IgniteInternalTx> REDUCER =
        new IgniteReducer<IgniteInternalTx, IgniteInternalTx>() {
            @Override public boolean collect(IgniteInternalTx e) {
                return true;
            }

            @Override public IgniteInternalTx reduce() {
                // Nothing to aggregate.
                return null;
            }
        };

    /** Logger. */
    protected static IgniteLogger log;

    /** Context. */
    protected GridCacheSharedContext<?, ?> cctx;

    /** Future ID. */
    protected IgniteUuid futId;

    /** Transaction. */
    @GridToStringInclude
    protected GridNearTxLocal tx;

    /** Error. */
    @GridToStringExclude
    protected AtomicReference<Throwable> err = new AtomicReference<>(null);

    /** Trackable flag. */
    protected boolean trackable = true;

    /** Full information about transaction nodes mapping. */
    protected GridDhtTxMapping txMapping;

    /**
     * @param cctx Context.
     * @param tx Transaction.
     */
    public GridNearTxPrepareFutureAdapter(GridCacheSharedContext cctx, final GridNearTxLocal tx) {
        super(cctx.kernalContext(), REDUCER);

        assert cctx != null;
        assert tx != null;

        this.cctx = cctx;
        this.tx = tx;

        futId = IgniteUuid.randomUuid();

        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridNearTxPrepareFutureAdapter.class);
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid futureId() {
        return futId;
    }

    /** {@inheritDoc} */
    @Override public GridCacheVersion version() {
        return tx.xidVersion();
    }

    /** {@inheritDoc} */
    @Override public void markNotTrackable() {
        trackable = false;
    }

    /** {@inheritDoc} */
    @Override public boolean trackable() {
        return trackable;
    }

    /**
     * Prepares transaction.
     */
    public abstract void prepare();

    /**
     * @param nodeId Sender.
     * @param res Result.
     */
    public abstract void onResult(UUID nodeId, GridNearTxPrepareResponse res);

    /**
     * Checks if mapped transaction can be committed on one phase.
     * One-phase commit can be done if transaction maps to one primary node and not more than one backup.
     */
    protected final void checkOnePhase() {
        if (tx.storeUsed())
            return;

        Map<UUID, Collection<UUID>> map = txMapping.transactionNodes();

        if (map.size() == 1) {
            Map.Entry<UUID, Collection<UUID>> entry = F.firstEntry(map);

            assert entry != null;

            Collection<UUID> backups = entry.getValue();

            if (backups.size() <= 1)
                tx.onePhaseCommit(true);
        }
    }

    /**
     * @param m Mapping.
     * @param res Response.
     */
    protected final void onPrepareResponse(GridDistributedTxMapping m, GridNearTxPrepareResponse res) {
        if (res == null)
            return;

        assert res.error() == null : res;
        assert F.isEmpty(res.invalidPartitions()) : res;

        for (Map.Entry<IgniteTxKey, CacheVersionedValue> entry : res.ownedValues().entrySet()) {
            IgniteTxEntry txEntry = tx.entry(entry.getKey());

            assert txEntry != null;

            GridCacheContext cacheCtx = txEntry.context();

            while (true) {
                try {
                    if (cacheCtx.isNear()) {
                        GridNearCacheEntry nearEntry = (GridNearCacheEntry)txEntry.cached();

                        CacheVersionedValue tup = entry.getValue();

                        nearEntry.resetFromPrimary(tup.value(), tx.xidVersion(),
                            tup.version(), m.node().id(), tx.topologyVersion());
                    }
                    else if (txEntry.cached().detached()) {
                        GridDhtDetachedCacheEntry detachedEntry = (GridDhtDetachedCacheEntry)txEntry.cached();

                        CacheVersionedValue tup = entry.getValue();

                        detachedEntry.resetFromPrimary(tup.value(), tx.xidVersion());
                    }

                    break;
                }
                catch (GridCacheEntryRemovedException ignored) {
                    // Retry.
                }
            }
        }

        tx.implicitSingleResult(res.returnValue());

        for (IgniteTxKey key : res.filterFailedKeys()) {
            IgniteTxEntry txEntry = tx.entry(key);

            assert txEntry != null : "Missing tx entry for write key: " + key;

            txEntry.op(NOOP);

            assert txEntry.context() != null;

            ExpiryPolicy expiry = txEntry.context().expiryForTxEntry(txEntry);

            if (expiry != null)
                txEntry.ttl(CU.toTtl(expiry.getExpiryForAccess()));
        }

        if (!m.empty()) {
            GridCacheVersion writeVer = res.writeVersion();

            if (writeVer == null)
                writeVer = res.dhtVersion();

            // Register DHT version.
            tx.addDhtVersion(m.node().id(), res.dhtVersion(), writeVer);

            m.dhtVersion(res.dhtVersion(), writeVer);

            if (m.near())
                tx.readyNearLocks(m, res.pending(), res.committedVersions(), res.rolledbackVersions());
        }
    }
}