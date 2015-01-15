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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.testframework.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class GridCacheQueryIndexingDisabledSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setCacheMode(GridCacheMode.PARTITIONED);
        ccfg.setQueryIndexEnabled(false);

        return ccfg;
    }

    /**
     * @param c Closure.
     */
    private void doTest(Callable<Object> c) {
        GridTestUtils.assertThrows(log, c, IgniteCheckedException.class, "Indexing is disabled for cache: null");
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSqlFieldsQuery() throws IgniteCheckedException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return cache().queries().createSqlFieldsQuery("select * from dual").execute()
                    .get();
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testTextQuery() throws IgniteCheckedException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return cache().queries().createFullTextQuery(String.class, "text")
                    .execute().get();
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testSqlQuery() throws IgniteCheckedException {
        doTest(new Callable<Object>() {
            @Override public Object call() throws IgniteCheckedException {
                return cache().queries().createSqlQuery(String.class, "1 = 1")
                    .execute().get();
            }
        });
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testScanQuery() throws IgniteCheckedException {
        GridCacheQuery<Map.Entry<String, Integer>> qry = cache().queries().createScanQuery(null);

        qry.execute().get();
    }
}
