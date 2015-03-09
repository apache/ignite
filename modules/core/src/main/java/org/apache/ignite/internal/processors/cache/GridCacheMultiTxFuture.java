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

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Future which waits for completion of one or more transactions.
 */
public final class GridCacheMultiTxFuture<K, V> extends GridFutureAdapter<Boolean> {
    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Logger. */
    private static IgniteLogger log;

    /** */
    private Set<IgniteInternalTx<K, V>> remainingTxs;

    /**
     * @param cctx Cache context.
     */
    public GridCacheMultiTxFuture(GridCacheContext<K, V> cctx) {
        if (log == null)
            log = U.logger(cctx.kernalContext(), logRef, GridCacheMultiTxFuture.class);
    }

    /**
     * @param tx Transaction to add.
     */
    public void addTx(IgniteInternalTx<K, V> tx) {
        if (remainingTxs == null)
            remainingTxs = new GridConcurrentHashSet<>();

        remainingTxs.add(tx);
    }

    /**
     * Initializes this future.
     */
    public void init() {
        if (remainingTxs == null) {
            remainingTxs = Collections.emptySet();

            onDone(true);
        }
        else {
            assert !remainingTxs.isEmpty();

            for (final IgniteInternalTx<K, V> tx : remainingTxs) {
                if (!tx.done()) {
                    tx.finishFuture().listen(new CI1<IgniteInternalFuture<IgniteInternalTx>>() {
                        @Override public void apply(IgniteInternalFuture<IgniteInternalTx> t) {
                            remainingTxs.remove(tx);

                            checkRemaining();
                        }
                    });
                }
                else
                    remainingTxs.remove(tx);
            }

            checkRemaining();
        }
    }

    /**
     * @return {@code True} if remaining set is empty.
     */
    private boolean checkRemaining() {
        if (remainingTxs.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Finishing multi-tx future: " + this);

            onDone(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMultiTxFuture.class, this,
            "remaining", F.viewReadOnly(remainingTxs, CU.<K, V>tx2xidVersion()),
            "super", super.toString()
        );
    }
}
