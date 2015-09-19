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

package org.apache.ignite.internal.processors.cache.reducefields;

import java.util.List;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 * Reduce fields queries tests for partitioned cache.
 */
public class GridCacheReduceFieldsQueryPartitionedSelfTest extends GridCacheAbstractReduceFieldsQuerySelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncludeBackups() throws Exception {
        CacheQuery<List<?>> qry = ((IgniteKernal)grid(0)).getCache(null).context().queries().
            createSqlFieldsQuery("select age from Person", false);

        qry.includeBackups(true);

        int sum = 0;

        for (IgniteBiTuple<Integer, Integer> tuple : qry.execute(new AverageRemoteReducer()).get())
            sum += tuple.get1();

        // One backup, so sum is two times greater
        assertEquals("Sum", 200, sum);
    }
}