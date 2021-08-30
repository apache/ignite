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

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.security.AbstractCacheOperationPermissionCheckTest;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.plugin.security.SecurityException;
import org.apache.ignite.plugin.security.SecurityPermissionSetBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static java.util.Collections.singleton;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Test cache permission for Entry processor.
 */
@RunWith(JUnit4.class)
public class EntryProcessorPermissionCheckTest extends AbstractCacheOperationPermissionCheckTest {
    /** */
    @Test
    public void test() throws Exception {
        IgniteEx verifierNode = startGrid("verifier_node",
            SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, CACHE_READ)
                .appendCachePermissions(FORBIDDEN_CACHE, CACHE_READ).build(), false);

        IgniteEx srvNode = startGrid("server_node",
            SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, CACHE_READ, CACHE_PUT)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), false);

        IgniteEx clientNode = startGrid("client_node",
            SecurityPermissionSetBuilder.create()
                .appendCachePermissions(CACHE_NAME, CACHE_PUT, CACHE_READ)
                .appendCachePermissions(FORBIDDEN_CACHE, EMPTY_PERMS).build(), true);

        awaitPartitionMapExchange();

        Stream.of(srvNode, clientNode).forEach(n ->
            operations(n).forEach(c -> {
                runOperation(verifierNode, c);

                runForbiddenOperation(verifierNode, c);
            })
        );
    }

    /** */
    private void runOperation(Ignite verifierNode, BiConsumer<String, T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        c.accept(CACHE_NAME, entry);

        assertThat(verifierNode.<String, Integer>cache(CACHE_NAME).get(entry.getKey()), is(entry.getValue()));
    }

    /** */
    private void runForbiddenOperation(Ignite verifierNode, BiConsumer<String, T2<String, Integer>> c) {
        T2<String, Integer> entry = entry();

        assertThrowsWithCause(() -> c.accept(FORBIDDEN_CACHE, entry), SecurityException.class);

        assertNull(verifierNode.cache(FORBIDDEN_CACHE).get(entry.getKey()));
    }

    /**
     * @return Collection of operations to invoke entry processor.
     */
    private List<BiConsumer<String, T2<String, Integer>>> operations(final Ignite node) {
        return Arrays.asList(
            (cacheName, t) -> node.cache(cacheName).invoke(t.getKey(), processor(t)),
            (cacheName, t) -> node.cache(cacheName).invokeAll(singleton(t.getKey()), processor(t)),
            (cacheName, t) -> node.cache(cacheName).invokeAsync(t.getKey(), processor(t)).get(),
            (cacheName, t) -> node.cache(cacheName).invokeAllAsync(singleton(t.getKey()), processor(t)).get()
        );
    }

    /**
     * @param t T2.
     */
    private CacheEntryProcessor<Object, Object, Object> processor(T2<String, Integer> t) {
        return (entry, o) -> {
            entry.setValue(t.getValue());

            return null;
        };
    }
}
