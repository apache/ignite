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

package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.util.lang.IgniteThrowableConsumer;

/**
 * Utility class for testing indexing.
 */
class IndexingTestUtils {
    /** A do-nothing {@link CacheDataRow} consumer. */
    static final IgniteThrowableConsumer<CacheDataRow> DO_NOTHING_CACHE_DATA_ROW_CONSUMER = row -> { };

    /**
     * Private constructor.
     */
    private IndexingTestUtils() {
        // No-op.
    }

    /**
     * Getting local instance name of the node.
     *
     * @param kernalCtx Kernal context.
     * @return Local instance name.
     */
    static String nodeName(GridKernalContext kernalCtx) {
        return kernalCtx.igniteInstanceName();
    }

    /**
     * Getting local instance name of the node.
     *
     * @param n Node.
     * @return Local instance name.
     */
    static String nodeName(IgniteEx n) {
        return nodeName(n.context());
    }

    /**
     * Getting local instance name of the node.
     *
     * @param cacheCtx Cache context.
     * @return Local instance name.
     */
    static String nodeName(GridCacheContext cacheCtx) {
        return nodeName(cacheCtx.kernalContext());
    }
}
