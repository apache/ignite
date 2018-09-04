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
public class IndexingIgniteCachePartitionLossPolicySelfTest extends IgniteCachePartitionLossPolicySelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration<Integer, Integer> cacheConfiguration() {
        CacheConfiguration<Integer, Integer> ccfg = super.cacheConfiguration();

        ccfg.setIndexedTypes(Integer.class, Integer.class);

        return ccfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void validateQuery(boolean safe, int part, Ignite node) {
        IgniteCache cache = node.cache(CACHE_NAME);

        Collection<Integer> lost = cache.lostPartitions();

        // 1. Check query against all partitions.
        if (safe) {
            try {
                executeQuery(null, node);

                fail("Exception is not thrown.");
            }
            catch (Exception e) {
                // TODO
                System.out.println("EXPECTED ERROR: " + e);
            }
        }
        else {
            executeQuery(null, node);
        }

        // 2. Check query against LOST partition.
        if (safe) {
            try {
                executeQuery(part, node);

                fail("Exception is not thrown.");
            }
            catch (Exception e) {
                // TODO
                System.out.println("EXPECTED ERROR: " + e);
            }
        }
        else {
            executeQuery(part, node);
        }

        // 3. Check local query against LOST partition.
        if (safe) {
            try {
                executeQuery(part, node);

                fail("Exception is not thrown.");
            }
            catch (Exception e) {
                // TODO
                System.out.println("EXPECTED ERROR: " + e);
            }
        }
        else {
            executeQuery(part, node);
        }

        // 4. Check query on remaining partition.
        Integer remainingPart = null;

        for (int i = 0; i < node.affinity(CACHE_NAME).partitions(); i++) {
            if (lost.contains(i))
                continue;

            remainingPart = i;

            break;
        }

        if (remainingPart != null) {
            executeQuery(remainingPart, node);
        }
    }

    /**
     * Execute SQL query on a given node.
     *
     * @param part Partition.
     * @param node Node.
     */
    private static void executeQuery(Integer part, Ignite node) {
        executeQuery(part, node, false);
    }

    /**
     * Execute SQL query on a given node.
     *
     * @param part Partition.
     * @param node Node.
     * @param loc Local flag.
     */
    private static void executeQuery(Integer part, Ignite node, boolean loc) {
        IgniteCache cache = node.cache(CACHE_NAME);

        SqlFieldsQuery qry = new SqlFieldsQuery("SELECT * FROM Integer");

        if (part != null)
            qry.setPartitions((int)part);

        if (loc)
            qry.setLocal(true);

        cache.query(qry).getAll();
    }
}
