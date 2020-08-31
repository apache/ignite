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

package org.apache.ignite.internal.processors.cache.verify;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.internal.processors.cache.GridCacheEntryRemovedException;
import org.apache.ignite.internal.processors.cache.GridCacheMvccCandidate;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxEntry;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 */
public class ContentionClosure implements IgniteCallable<ContentionInfo> {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Ignite. */
    @IgniteInstanceResource
    protected transient IgniteEx ignite;

    /** */
    private int minQueueSize;

    /** */
    private int maxPrint;

    /**
     * @param minQueueSize Min candidate queue size to account.
     * @param maxPrint Max entries to print.
     */
    public ContentionClosure(int minQueueSize, int maxPrint) {
        this.minQueueSize = minQueueSize;
        this.maxPrint = maxPrint;
    }

    /** {@inheritDoc} */
    @Override public ContentionInfo call() throws Exception {
        final IgniteTxManager tm = ignite.context().cache().context().tm();

        final Collection<IgniteInternalTx> activeTxs = tm.activeTransactions();

        ContentionInfo ci = new ContentionInfo();

        ci.setNode(ignite.localNode());
        ci.setEntries(new ArrayList<>());

        for (IgniteInternalTx tx : activeTxs) {
            if (ci.getEntries().size() == maxPrint)
                break;

            // Show only primary txs.
            if (tx.local()) {
                IgniteTxLocalAdapter tx0 = (IgniteTxLocalAdapter)tx;

                final IgniteTxLocalState state0 = tx0.txState();

                if (!(state0 instanceof IgniteTxStateImpl))
                    continue;

                final IgniteTxStateImpl state = (IgniteTxStateImpl)state0;

                final Collection<IgniteTxEntry> entries = state.allEntriesCopy();

                IgniteTxEntry bad = null;

                int qSize = 0;

                for (IgniteTxEntry entry : entries) {
                    Collection<GridCacheMvccCandidate> locs;

                    GridCacheEntryEx cached = entry.cached();

                    while (true) {
                        try {
                            locs = cached.localCandidates();

                            break;
                        }
                        catch (GridCacheEntryRemovedException ignored) {
                            cached = entry.context().cache().entryEx(entry.key());
                        }
                    }

                    if (locs != null)
                        qSize += locs.size();

                    final Collection<GridCacheMvccCandidate> rmts = cached.remoteMvccSnapshot();

                    if (rmts != null)
                        qSize += rmts.size();

                    if (qSize >= minQueueSize) {
                        bad = entry;

                        break;
                    }
                    else
                        qSize = 0;
                }

                if (bad != null) {
                    StringBuilder b = new StringBuilder();

                    b.append("TxEntry [cacheId=").append(bad.cacheId()).
                        append(", key=").append(bad.key()).
                        append(", queue=").append(qSize).
                        append(", op=").append(bad.op()).
                        append(", val=").append(bad.value()).
                        append(", tx=").append(CU.txString(tx)).
                        append(", other=[");

                    final IgniteTxState st = tx.txState();

                    if (st instanceof IgniteTxStateImpl) {
                        IgniteTxStateImpl st0 = (IgniteTxStateImpl)st;

                        final Collection<IgniteTxEntry> cp = st0.allEntriesCopy();

                        for (IgniteTxEntry entry : cp) {
                            if (entry == bad)
                                continue;

                            b.append(entry.toString()).append('\n');
                        }
                    }

                    b.append("]]");

                    ci.getEntries().add(b.toString());
                }
            }
        }

        return ci;
    }
}
