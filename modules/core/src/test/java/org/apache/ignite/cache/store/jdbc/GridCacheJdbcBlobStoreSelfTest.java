/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cache.store.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.ignite.testframework.junits.cache.GridAbstractCacheStoreSelfTest;

/**
 * Cache store test.
 */
public class GridCacheJdbcBlobStoreSelfTest
    extends GridAbstractCacheStoreSelfTest<CacheJdbcBlobStore<Object, Object>> {
    /**
     * @throws Exception If failed.
     */
    public GridCacheJdbcBlobStoreSelfTest() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        try (Connection c = DriverManager.getConnection(CacheJdbcBlobStore.DFLT_CONN_URL, null, null)) {
            try (Statement s = c.createStatement()) {
                s.executeUpdate("drop table ENTRIES");
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected CacheJdbcBlobStore<Object, Object> store() {
        return new CacheJdbcBlobStore<>();
    }
}