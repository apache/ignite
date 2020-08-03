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

package org.apache.ignite.internal.processors.security.cache;

import java.util.function.Consumer;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests check a cache permission for continuous queries.
 */
@RunWith(JUnit4.class)
public class ContinuousQueryPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** Test server node name. */
    private static final String SRV = "srv_test_node";

    /** Test client node name. */
    private static final String CLNT = "cnt_test_node";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite srv = startGridAllowAll("server");

        startGrid(SRV, permissions(), true);

        startGrid(CLNT, permissions(), true);

        srv.cluster().state(ClusterState.ACTIVE);
    }

    /**
     * Tests {@link ContinuousQuery} from a client.
     */
    @Test
    public void testClientContinuousQuery() {
        check(n -> startContinuousQuery(grid(CLNT), n));
    }

    /**
     * Tests {@link ContinuousQuery} from a server.
     */
    @Test
    public void testServerContinuousQuery() {
        check(n -> startContinuousQuery(grid(SRV), n));
    }

    /**
     * Tests {@link ContinuousQueryWithTransformer} from a client.
     */
    @Test
    public void testClientContinuousQueryWithTransformer() {
        check(n -> startContinuousQueryWithTransformer(grid(CLNT), n));
    }

    /**
     * Tests {@link ContinuousQueryWithTransformer} from a server.
     */
    @Test
    public void testServerContinuousQueryWithTransformer() {
        check(n -> startContinuousQueryWithTransformer(grid(SRV), n));
    }

    /**
     * @param c Consumer that accepts a cache name.
     */
    private void check(final Consumer<String> c) {
        c.accept(CACHE_NAME);

        assertThrowsWithCause(() -> c.accept(FORBIDDEN_CACHE), SecurityException.class);
    }

    /**
     * Starts {@link ContinuousQuery}.
     *
     * @param node Ignie node.
     * @param cacheName Cache name.
     */
    private void startContinuousQuery(Ignite node, String cacheName) {
        ContinuousQuery<String, Integer> q = new ContinuousQuery<>();

        q.setLocalListener(e -> {/* No-op. */});

        try (QueryCursor<Cache.Entry<String, Integer>> cur = node.cache(cacheName).query(q)) {
            // No-op.
        }
    }

    /**
     * Starts {@link ContinuousQueryWithTransformer}.
     *
     * @param node Ignite node.
     * @param cacheName Cache name.
     */
    private void startContinuousQueryWithTransformer(Ignite node, String cacheName) {
        ContinuousQueryWithTransformer<String, Integer, String> q = new ContinuousQueryWithTransformer<>();

        q.setLocalListener(e -> {/* No-op. */});

        q.setRemoteTransformerFactory(() -> e -> "value");

        try (QueryCursor<Cache.Entry<String, Integer>> cur = node.cache(cacheName).query(q)) {
            // No-op.
        }
    }

    /**
     * @return Subject Ignite permissions.
     */
    private SecurityPermissionSet permissions() {
        return SecurityPermissionSetBuilder.create()
            .appendCachePermissions(CACHE_NAME, CACHE_READ)
            .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build();
    }
}
