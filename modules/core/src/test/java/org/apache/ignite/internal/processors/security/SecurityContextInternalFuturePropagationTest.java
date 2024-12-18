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

package org.apache.ignite.internal.processors.security;

import java.security.Permissions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientAuthorizationException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.security.impl.TestSecurityData;
import org.apache.ignite.internal.processors.security.impl.TestSecurityPluginProvider;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.security.SecurityPermissionSet;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.util.Collections.singletonMap;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.internal.processors.cache.distributed.GridCacheModuloAffinityFunction.IDX_ATTR;
import static org.apache.ignite.plugin.security.SecurityPermission.ADMIN_CLUSTER_STATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_CREATE;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_PUT;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_READ;
import static org.apache.ignite.plugin.security.SecurityPermission.CACHE_REMOVE;
import static org.apache.ignite.plugin.security.SecurityPermission.JOIN_AS_SERVER;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.NO_PERMISSIONS;
import static org.apache.ignite.plugin.security.SecurityPermissionSetBuilder.create;

/** */
@RunWith(Parameterized.class)
public class SecurityContextInternalFuturePropagationTest extends GridCommonAbstractTest {
    /** */
    private static final int PRELOADED_KEY_CNT = 100;

    /** */
    private static final AtomicInteger KEY_CNTR = new AtomicInteger();

    /** */
    private static final List<Function<ClientCache<Object, Object>, IgniteClientFuture<?>>> CACHE_OPERATIONS = Arrays.asList(
        cache -> cache.getAllAsync(ImmutableSet.of(nextKey(), nextKey())), // 0
        cache -> cache.getAndPutAsync(nextKey(), 0), // 1
        cache -> cache.getAndPutIfAbsentAsync(nextKey(), 0), // 2
        cache -> cache.getAndPutIfAbsentAsync(PRELOADED_KEY_CNT + nextKey(), 0), // 3
        cache -> cache.getAndRemoveAsync(nextKey()), // 4
        cache -> cache.getAndReplaceAsync(nextKey(), 0), // 5
        cache -> cache.getAsync(nextKey()), // 6
        cache -> cache.putAllAsync(new HashMap<>() {{ put(nextKey(), 0); put(nextKey(), 0); }}), // 7
        cache -> cache.putAsync(nextKey(), 0), // 8
        cache -> cache.putIfAbsentAsync(PRELOADED_KEY_CNT + nextKey(), 0), // 9
        cache -> cache.removeAsync(nextKey()), // 10
        cache -> {
            int key = nextKey();
            return cache.removeAsync(key, key);
        }, // 11
        cache -> cache.replaceAsync(nextKey(), 0), // 12
        cache -> {
            int key = nextKey();

            return cache.replaceAsync(key, key, 0);
        } // 13
    );

    /** */
    @Parameterized.Parameter()
    public Function<ClientCache<Object, Object>, IgniteClientFuture<?>> op;

    /** */
    @Parameterized.Parameters()
    public static Iterable<Object[]> data() {
        List<Object[]> res = new ArrayList<>();

        for (Function<ClientCache<Object, Object>, IgniteClientFuture<?>> op : CACHE_OPERATIONS)
            res.add(new Object[] {op});

        return res;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setCommunicationSpi(new TestRecordingCommunicationSpi())
            .setUserAttributes(singletonMap(IDX_ATTR, getTestIgniteInstanceIndex(igniteInstanceName)))
            .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                .setThreadPoolSize(1))
            .setPluginProviders(new TestSecurityPluginProvider(
                igniteInstanceName,
                "",
                create()
                    .defaultAllowAll(false)
                    .appendSystemPermissions(JOIN_AS_SERVER, ADMIN_CLUSTER_STATE)
                    .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_CREATE)
                    .build(),
                null,
                false,
                userData("forbidden_client", NO_PERMISSIONS),
                userData("allowed_client", create()
                    .defaultAllowAll(false)
                    .appendCachePermissions(DEFAULT_CACHE_NAME, CACHE_READ, CACHE_PUT, CACHE_REMOVE)
                    .build())
            ));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    @Test
    public void testSecurityContextInternalFuturePropagation() throws Exception {
        IgniteEx ignite = startGrids(2);

        prepareCache(ignite);

        TestRecordingCommunicationSpi spi = TestRecordingCommunicationSpi.spi(grid(1));

        spi.blockMessages(GridDhtPartitionsSingleMessage.class, ignite.name());

        IgniteInternalFuture<IgniteEx> joinFut = GridTestUtils.runAsync(() -> startGrid(2));

        spi.waitForBlocked();

        try (
            IgniteClient allowedCli = startClient("allowed_client");
            IgniteClient forbiddenCli = startClient("forbidden_client")
        ) {
            ClientCache<Object, Object> allowedCliCache = allowedCli.cache(DEFAULT_CACHE_NAME);
            ClientCache<Object, Object> forbiddenCliCache = forbiddenCli.cache(DEFAULT_CACHE_NAME);

            IgniteClientFuture<?> op0Fut = op.apply(allowedCliCache);

            // The following operation previously was executed with security context associated with the joining node user.
            IgniteClientFuture<?> op1Fut = op.apply(allowedCliCache);

            // Simulates failure of a chained operation.
            IgniteClientFuture<?> op2Fut = op.apply(forbiddenCliCache);

            // The failure of one operation in a chain should not leave the entire chain in a broken state.
            IgniteClientFuture<?> op3Fut = op.apply(allowedCliCache);

            spi.stopBlock();

            joinFut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

            op0Fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
            op1Fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

            try {
                op2Fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);

                fail();
            }
            catch (Exception e) {
                assertTrue(X.hasCause(e, "Authorization failed", ClientException.class)
                    || X.hasCause(e, "User is not authorized to perform this operation", ClientAuthorizationException.class));
            }

            op3Fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
        }
    }

    /** */
    private void prepareCache(IgniteEx ignite) throws Exception {
        ignite.createCache(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL)
            .setReadFromBackup(false)
            .setBackups(1)
            .setAffinity(new GridCacheModuloAffinityFunction(2, 1)));

        awaitPartitionMapExchange();

        try (IgniteClient cli = startClient("allowed_client")) {
            for (int i = 0; i < PRELOADED_KEY_CNT; i++) {
                cli.cache(DEFAULT_CACHE_NAME).put(i, i);
            }
        }
    }

    /** */
    private static int nextKey() {
        return KEY_CNTR.incrementAndGet();
    }

    /** */
    private static IgniteClient startClient(String login) {
        return Ignition.startClient(new ClientConfiguration()
            .setAddresses("127.0.0.1:10800")
            .setUserName(login)
            .setUserPassword(""));
    }

    /** */
    private static TestSecurityData userData(String login, SecurityPermissionSet perms) {
        return new TestSecurityData(
            login,
            "",
            perms,
            new Permissions()
        );
    }
}
