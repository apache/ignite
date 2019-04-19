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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class H2StatementCacheSelfTest extends AbstractIndexingCommonTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testEviction() throws Exception {
        H2StatementCache stmtCache = new H2StatementCache(1);
        H2CachedStatementKey key1 = new H2CachedStatementKey("", "1");
        PreparedStatement stmt1 = stmt();
        stmtCache.put(key1, stmt1);

        assertSame(stmt1, stmtCache.get(key1));

        stmtCache.put(new H2CachedStatementKey("mydb", "2"), stmt());

        assertNull(stmtCache.get(key1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLruEvictionInStoreOrder() throws Exception {
        H2StatementCache stmtCache = new H2StatementCache(2);

        H2CachedStatementKey key1 = new H2CachedStatementKey("", "1");
        H2CachedStatementKey key2 = new H2CachedStatementKey("", "2");
        stmtCache.put(key1, stmt());
        stmtCache.put(key2, stmt());

        stmtCache.put(new H2CachedStatementKey("", "3"), stmt());

        assertNull(stmtCache.get(key1));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testLruEvictionInAccessOrder() throws Exception {
        H2StatementCache stmtCache = new H2StatementCache(2);

        H2CachedStatementKey key1 = new H2CachedStatementKey("", "1");
        H2CachedStatementKey key2 = new H2CachedStatementKey("", "2");
        stmtCache.put(key1, stmt());
        stmtCache.put(key2, stmt());
        stmtCache.get(key1);

        stmtCache.put(new H2CachedStatementKey("", "3"), stmt());

        assertNull(stmtCache.get(key2));
    }

    /**
     *
     */
    private static PreparedStatement stmt() {
        return new PreparedStatementExImpl(null);
    }
}
