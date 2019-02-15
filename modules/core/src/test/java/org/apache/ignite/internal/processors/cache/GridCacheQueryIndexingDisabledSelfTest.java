/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
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
