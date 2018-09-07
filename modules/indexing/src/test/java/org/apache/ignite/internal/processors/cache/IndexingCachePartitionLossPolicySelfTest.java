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

import java.util.Collection;

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
    @SuppressWarnings("unchecked")
    @Override protected void validateQuery(boolean safe, int part, Ignite node) {
        // Get node lost and remaining partitions.
        IgniteCache cache = node.cache(CACHE_NAME);

        Collection<Integer> lostParts = cache.lostPartitions();

        Integer remainingPart = null;

        for (int i = 0; i < node.affinity(CACHE_NAME).partitions(); i++) {
            if (lostParts.contains(i))
                continue;

            remainingPart = i;

            break;
        }

        // Determine whether local query should be executed on that node.
        boolean execLocQry = false;

        for (int nodePrimaryPart : node.affinity(CACHE_NAME).primaryPartitions(node.cluster().localNode())) {
            if (part == nodePrimaryPart) {
                execLocQry = true;

                break;
            }
        }

        // 1. Check query against all partitions.
        validateQuery0(safe, node, false);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-7039
//        if (execLocQry)
//            validateQuery0(safe, node, true);

        // 2. Check query against LOST partition.
        validateQuery0(safe, node, false, part);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-7039
//        if (execLocQry)
//            validateQuery0(safe, node, true, part);

        // 3. Check query on remaining partition.
        if (remainingPart != null) {
            executeQuery(node, false, remainingPart);

            // 4. Check query over two partitions - normal and LOST.
            validateQuery0(safe, node, false, part, remainingPart);
        }
    }

    /**
     * Query validation routine.
     *
     * @param safe Safe flag.
     * @param node Node.
     * @param loc Local flag.
     * @param parts Partitions.
     */
    private void validateQuery0(boolean safe, Ignite node, boolean loc, int... parts) {
        if (safe) {
            try {
                executeQuery(node, loc, parts);

                fail("Exception is not thrown.");
            }
            catch (Exception e) {
                assertTrue(e.getMessage(), e.getMessage() != null &&
                    e.getMessage().contains("Failed to execute query because cache partition has been lost"));
            }
        }
        else {
            executeQuery(node, loc, parts);
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
        IgniteCache cache = node.cache(CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM Integer");

        if (parts != null && parts.length != 0)
            qry.setPartitions(parts);

        if (loc)
            qry.setLocal(true);

        cache.query(qry).getAll();
    }
}
