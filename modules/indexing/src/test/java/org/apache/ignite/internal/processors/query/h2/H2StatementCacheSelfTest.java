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

package org.apache.ignite.internal.processors.query.h2;

import java.sql.PreparedStatement;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class H2StatementCacheSelfTest extends GridCommonAbstractTest {

    /**
     * @throws Exception If failed.
     */
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