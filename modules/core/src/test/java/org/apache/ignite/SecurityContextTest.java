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

package org.apache.ignite;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.security.GridSecurityProcessor;
import org.apache.ignite.internal.processors.security.SecurityContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.security.SecurityCredentials;
import org.apache.ignite.spi.discovery.TestReconnectSecurityPluginProvider;
import org.apache.ignite.spi.discovery.tcp.TestReconnectProcessor;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/** */
public class SecurityContextTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setFailureHandler(new StopNodeOrHaltFailureHandler())
            .setPluginProviders(new TestReconnectSecurityPluginProvider() {
                @Override protected GridSecurityProcessor securityProcessor(GridKernalContext ctx) {
                    return new TestReconnectProcessor(ctx) {
                        @Override public SecurityContext securityContext(UUID subjId) {
                            throw new IllegalStateException("Unexpected subjId[subjId=" + subjId + ", localNodeId=" + ctx.localNodeId() + ']');
                        }

                        @Override public SecurityContext authenticateNode(ClusterNode node, SecurityCredentials cred) {
                            return new TestSecurityContext(new TestSecuritySubject(node.id()));
                        }
                    };
                }
            });
    }

    /** */
    @Test
    public void test() throws Exception {
        startGrids(2);

        grid(0).createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setBackups(1)
            .setWriteSynchronizationMode(FULL_SYNC)
            .setAtomicityMode(TRANSACTIONAL));

        awaitPartitionMapExchange();

        pauseDiscoveryNotificationWorkerFor(grid(1), Duration.ofMillis(500));

        IgniteEx cli = startClientGrid(2);

        IgniteCache<Integer, Integer> cache = cli.cache(DEFAULT_CACHE_NAME);

        for (int j = 0; j < 100; j++)
            cache.put(j, j);
    }

    /** */
    private void pauseDiscoveryNotificationWorkerFor(IgniteEx ignite, Duration pause) throws Exception {
        Object discoWrk = U.field(ignite.context().discovery(), "discoNtfWrk");

        Method submitMethod = discoWrk.getClass().getDeclaredMethod("submit", GridFutureAdapter.class, Runnable.class);

        submitMethod.setAccessible(true);

        Runnable pauseTask = () -> {
            try {
                U.sleep(pause.toMillis());
            }
            catch (IgniteInterruptedCheckedException ignored) {
               // No-op.
            }
        };

        submitMethod.invoke(discoWrk, new GridFutureAdapter<>(), pauseTask);
    }
}
