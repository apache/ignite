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
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.CachePartitionLossStaleMapTest;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Runs {@link CachePartitionLossStaleMapTest} with a SQL query. A query fails if the node that runs it sees a partition
 * without an owner until the query's timeout.
 */
public class IndexingCachePartitionLossStaleMapTest extends CachePartitionLossStaleMapTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        for (CacheConfiguration<?, ?> ccfg : cfg.getCacheConfiguration())
            ccfg.setIndexedTypes(Integer.class, Integer.class);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void runQuery(Ignite node) {
        node.cache(CACHE).query(new SqlFieldsQuery("select count(*) from Integer").setTimeout(10, SECONDS)).getAll();
    }

    /** {@inheritDoc} */
    @Test
    @Ignore("No query runs under READ_WRITE_SAFE, so the test is the same as in CachePartitionLossStaleMapTest")
    @Override public void testStaleMapBeforeLostPartitionMarkedLost() throws Exception {
        super.testStaleMapBeforeLostPartitionMarkedLost();
    }
}
