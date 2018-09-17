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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Utilities related to H2 cache
 */
public class H2CacheUtils {

    /**
     *
     */
    private H2CacheUtils() {
    }

    /**
     * Check that given table has lazy cache and init it for such case.
     *
     * @param tbl Table to check on lazy cache
     */
    public static void checkAndInitLazyCache(GridH2Table tbl) {
        if(tbl != null && tbl.isCacheLazy()){
            String cacheName = tbl.cacheInfo().config().getName();

            GridKernalContext ctx = tbl.cacheInfo().context();

            ctx.cache().startLazyCache(cacheName);
        }
    }
}
