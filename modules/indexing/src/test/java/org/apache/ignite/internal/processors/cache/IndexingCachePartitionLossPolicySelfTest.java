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

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePartitionLossPolicySelfTest;

/**
 * Partition loss policy test with enabled indexing.
 */
public class IndexingCachePartitionLossPolicySelfTest extends IgniteCachePartitionLossPolicySelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        for (CacheConfiguration ccfg : cfg.getCacheConfiguration())
            ccfg.setIndexedTypes(Integer.class, Integer.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected List<?> runQuery(Ignite ig, String cacheName, boolean loc, int part) {
        IgniteCache cache = ig.cache(cacheName);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM Integer");

        if (part != -1)
            qry.setPartitions(part);

        // TODO https://issues.apache.org/jira/browse/IGNITE-7039
        // if (loc)
        //    qry.setLocal(true);

        return cache.query(qry).getAll();
    }
}
