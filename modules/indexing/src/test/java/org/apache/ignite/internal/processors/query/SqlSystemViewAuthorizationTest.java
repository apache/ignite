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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.sys.view.SqlSystemView;
import org.apache.ignite.internal.processors.security.AbstractSecurityTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;

import static org.apache.ignite.internal.processors.query.h2.SchemaManager.SQL_VIEWS_VIEW;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_VIEW;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests authorized access to {@link SqlSystemView}s.
 */
public class SqlSystemViewAuthorizationTest extends AbstractSecurityTest {
    /** */
    private static final SqlFieldsQuery QUERY_ALL = new SqlFieldsQuery("SELECT * FROM SYS." + SQL_VIEWS_VIEW);

    /** */
    private static final SqlFieldsQuery QUERY_COUNT = new SqlFieldsQuery("SELECT count(*) FROM SYS." + SQL_VIEWS_VIEW);

    /** @throws Exception If failed. */
    @Test
    public void testCanReadViewWhenPermitted() throws Exception {
        SecurityPermissionSet permSet = new SecurityPermissionSetBuilder()
            .appendSystemPermissions(ADMIN_VIEW)
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_CREATE, CACHE_READ)
            .build();

        IgniteEx server = startGrid("server", permSet, false);
        IgniteEx client = startGrid("client", permSet, true);

        checkReadViewSuceeds(server);
        checkReadViewSuceeds(client);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCannotReadViewWithoutPermission() throws Exception {
        SecurityPermissionSet permSet = new SecurityPermissionSetBuilder()
            .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_CREATE, CACHE_READ)
            .build();

        IgniteEx server = startGrid("server", permSet, false);
        IgniteEx client = startGrid("client", permSet, true);

        checkReadViewFails(server);
        checkReadViewFails(client);
    }

    /** */
    @Override public void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
        cleanPersistenceDir();
    }

    /** */
    private static void checkReadViewSuceeds(Ignite ignite) {
        IgniteCache<String, String> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (List<?> row : cache.query(QUERY_ALL).getAll())
            log.info(row.stream().map(Object::toString).collect(Collectors.joining("|")));

        log.info(cache.query(QUERY_COUNT).iterator().next().get(0).toString());
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    private static void checkReadViewFails(Ignite ignite) {
        IgniteCache<String, String> cache = ignite.getOrCreateCache(DEFAULT_CACHE_NAME);

        assertThrowsWithCause(() -> cache.query(QUERY_ALL).getAll(), SecurityException.class);
        assertThrowsWithCause(() -> cache.query(QUERY_COUNT).iterator().next().get(0), SecurityException.class);
    }
}
