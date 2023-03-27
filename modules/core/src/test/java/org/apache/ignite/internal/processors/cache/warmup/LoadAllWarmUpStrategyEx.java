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

package org.apache.ignite.internal.processors.cache.warmup;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.persistence.DataRegion;

import static java.util.Objects.nonNull;

/**
 * Extension {@link LoadAllWarmUpStrategy}.
 */
class LoadAllWarmUpStrategyEx extends LoadAllWarmUpStrategy {
    /** {@link #loadDataInfo} callback. */
    static volatile BiConsumer<String, Map<CacheGroupContext, List<LoadPartition>>> loadDataInfoCb;

    /**
     * Constructor.
     *
     * @param log       Logger.
     * @param grpCtxSup Cache group contexts supplier. Since {@link GridCacheProcessor} starts later.
     */
    public LoadAllWarmUpStrategyEx(IgniteLogger log, Supplier<Collection<CacheGroupContext>> grpCtxSup) {
        super(log, grpCtxSup);
    }

    /** {@inheritDoc} */
    @Override public Class configClass() {
        return LoadAllWarmUpConfigurationEx.class;
    }

    /** {@inheritDoc} */
    @Override protected Map<CacheGroupContext, List<LoadPartition>> loadDataInfo(
        DataRegion region
    ) throws IgniteCheckedException {
        Map<CacheGroupContext, List<LoadPartition>> loadDataInfo = super.loadDataInfo(region);

        if (nonNull(loadDataInfoCb))
            loadDataInfoCb.accept(region.config().getName(), loadDataInfo);

        return loadDataInfo;
    }
}
