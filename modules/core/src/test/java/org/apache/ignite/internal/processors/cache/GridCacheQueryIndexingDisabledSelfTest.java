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

import java.util.concurrent.Callable;
import javax.cache.CacheException;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class GridCacheQueryIndexingDisabledSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        return ccfg;
    }

    /**
     * @param c Closure.
     */
    private void doTest(Callable<Object> c, String expectedMsg) {
        GridTestUtils.assertThrows(log, c, CacheException.class, expectedMsg);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSqlFieldsQuery() throws IgniteCheckedException {
        // Should not throw despite the cache not having QueryEntities.
        jcache().query(new SqlFieldsQuery("select * from dual")).getAll();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testTextQuery() throws IgniteCheckedException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return jcache().query(new TextQuery<>(String.class, "text")).getAll();
            }
        }, "Indexing is disabled for cache: default");
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testSqlQuery() throws IgniteCheckedException {
        // Failure occurs not on validation stage, hence specific error message.
        doTest(new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return jcache().query(new SqlQuery<>(String.class, "1 = 1")).getAll();
            }
        }, "Failed to find SQL table for type: String");
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    @Test
    public void testScanQuery() throws IgniteCheckedException {
        jcache().query(new ScanQuery<>(null)).getAll();
    }
}
