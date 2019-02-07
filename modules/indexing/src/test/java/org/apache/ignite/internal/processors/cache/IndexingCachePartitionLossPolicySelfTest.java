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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.IgniteCachePartitionLossPolicySelfTest;

/**
 * Partition loss policy test with enabled indexing.
 */
public class IndexingCachePartitionLossPolicySelfTest extends IgniteCachePartitionLossPolicySelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> ccfg = super.cacheConfiguration();

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected void checkQueryPasses(Ignite node, boolean loc, int... parts) {
        executeQuery(node, loc, parts);
    }

    /** {@inheritDoc} */
    @Override protected void checkQueryFails(Ignite node, boolean loc, int... parts) {
        // TODO: Local queries ignore partition loss, see https://issues.apache.org/jira/browse/IGNITE-7039.
        if (loc)
            return;

        try {
            executeQuery(node, loc, parts);

            fail("Exception is not thrown.");
        }
        catch (Exception e) {
            boolean exp = e.getMessage() != null &&
                e.getMessage().contains("Failed to execute query because cache partition has been lost");

            if (!exp)
                throw e;
        }
    }

    /**
     * Execute SQL query on a given node.
     *
     * @param parts Partitions.
     * @param node Node.
     * @param loc Local flag.
     */
    private static void executeQuery(Ignite node, boolean loc, int... parts) {
        IgniteCache cache = node.cache(DEFAULT_CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM Integer");

        if (parts != null && parts.length != 0)
            qry.setPartitions(parts);

        if (loc)
            qry.setLocal(true);

        cache.query(qry).getAll();
    }
}
