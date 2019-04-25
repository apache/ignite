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

import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;

/**
 *
 */
class ClientCacheUpdateTimeout extends GridTimeoutObjectAdapter implements CachePartitionExchangeWorkerTask {
    /** */
    private final GridCacheSharedContext cctx;

    /**
     * @param cctx Context.
     * @param timeout Timeout.
     */
    ClientCacheUpdateTimeout(GridCacheSharedContext cctx, long timeout) {
        super(timeout);

        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public boolean skipForExchangeMerge() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void onTimeout() {
        if (!cctx.kernalContext().isStopping())
            cctx.exchange().addCustomTask(this);
    }
}
