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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.function.ToIntFunction;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.query.calcite.util.AbstractService;
import org.apache.ignite.internal.util.typedef.internal.CU;

/**
 *
 */
public class PartitionServiceImpl extends AbstractService implements PartitionService {
    /** */
    private GridCacheSharedContext<?,?> cacheSharedContext;

    /**
     * @param ctx Kernal.
     */
    public PartitionServiceImpl(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param cacheSharedContext Cache shared context.
     */
    public void cacheSharedContext(GridCacheSharedContext<?,?> cacheSharedContext) {
        this.cacheSharedContext = cacheSharedContext;
    }

    /** {@inheritDoc} */
    @Override public void onStart(GridKernalContext ctx) {
        cacheSharedContext(ctx.cache().context());
    }

    /** {@inheritDoc} */
    @Override public ToIntFunction<Object> partitionFunction(int cacheId) {
        if (cacheId == CU.UNDEFINED_CACHE_ID)
            return k -> k == null ? 0 : k.hashCode();

        AffinityFunction affinity = cacheSharedContext.cacheContext(cacheId).group().affinityFunction();

        return affinity::partition;
    }
}
