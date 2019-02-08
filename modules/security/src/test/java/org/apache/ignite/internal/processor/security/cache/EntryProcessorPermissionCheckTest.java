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

package org.apache.ignite.internal.processor.security.cache;

import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processor.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singleton;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Test cache permission for Entry processor.
 */
@RunWith(JUnit4.class)
public class EntryProcessorPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("server_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build());

        startGrid("client_node",
            builder()
                .appendCachePermissions(CACHE_NAME, CACHE_PUT, CACHE_READ)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), true);

        super.beforeTestsStarted();
    }

    /**
     *
     */
    @Test
    public void test() {
        IgniteEx srvNode = grid("server_node");

        IgniteEx clientNode = grid("client_node");

        invoke(srvNode);
        invoke(clientNode);

        invokeAll(srvNode);
        invokeAll(clientNode);

        invokeAsync(srvNode);
        invokeAsync(clientNode);

        invokeAsyncAll(srvNode);
        invokeAsyncAll(clientNode);
    }

    /**
     * @param node Node.
     */
    private void invoke(Ignite node) {
        assertAllowed(node, CACHE_NAME,
            (t) -> node.cache(CACHE_NAME).invoke(
                t.getKey(), processor(t)
            )
        );

        assertForbidden(node, CACHE_NAME,
            (t) -> node.cache(FORBIDDEN_CACHE).invoke(
                t.getKey(), processor(t)
            )
        );
    }

    /**
     * @param node Node.
     */
    private void invokeAll(Ignite node) {
        assertAllowed(node, CACHE_NAME,
            (t) -> node.cache(CACHE_NAME).invokeAll(
                singleton(t.getKey()), processor(t)
            )
        );

        assertForbidden(node, CACHE_NAME,
            (t) -> node.cache(FORBIDDEN_CACHE).invokeAll(
                singleton(t.getKey()), processor(t)
            )
        );
    }

    /**
     * @param node Node.
     */
    private void invokeAsync(Ignite node) {
        assertAllowed(node, CACHE_NAME,
            (t) -> node.cache(CACHE_NAME).invokeAsync(
                t.getKey(), processor(t)
            ).get()
        );

        assertForbidden(node, CACHE_NAME,
            (t) -> node.cache(FORBIDDEN_CACHE).invokeAsync(
                t.getKey(), processor(t)
            ).get()
        );
    }

    /**
     * @param node Node.
     */
    private void invokeAsyncAll(Ignite node) {
        assertAllowed(node, CACHE_NAME,
            (t) -> node.cache(CACHE_NAME).invokeAllAsync(
                singleton(t.getKey()), processor(t)
            ).get()
        );

        assertForbidden(node, CACHE_NAME,
            (t) -> node.cache(FORBIDDEN_CACHE).invokeAllAsync(
                singleton(t.getKey()), processor(t)
            ).get()
        );
    }

    /**
     * @param t T2.
     */
    private static CacheEntryProcessor<Object, Object, Object> processor(T2<String, Integer> t) {
        return (entry, o) -> {
            entry.setValue(t.getValue());

            return null;
        };
    }

    /**
     * @param c Consumer.
     */
    protected void assertAllowed(Ignite validator, String cacheName, Consumer<T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        c.accept(entry);

        assertThat(validator.cache(cacheName).get(entry.getKey()), is(entry.getValue()));
    }

    /**
     * @param c Consumer.
     */
    protected void assertForbidden(Ignite validator, String cacheName, Consumer<T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        try {
            c.accept(entry);

            fail("Should not happen.");
        }
        catch (Throwable e) {
            assertThat(X.cause(e, SecurityException.class), notNullValue());
        }

        assertThat(validator.cache(cacheName).get(entry.getKey()), nullValue());
    }
}
