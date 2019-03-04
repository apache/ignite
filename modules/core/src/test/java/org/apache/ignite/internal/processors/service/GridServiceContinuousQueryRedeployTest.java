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

package org.apache.ignite.internal.processors.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests whether concurrent service cancel and registering ContinuousQuery doesn't causes
 * service redeployment.
 */
public class GridServiceContinuousQueryRedeployTest extends GridCommonAbstractTest {
    /** */
    private static final String CACHE_NAME = "TEST_CACHE";

    /** */
    private static final String TEST_KEY = "TEST_KEY";

    /** */
    private static final String SERVICE_NAME = "service1";

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testServiceRedeploymentAfterCancel() throws Exception {
        final Ignite ignite = startGrid(0);

        final IgniteCache<Object, Object> managementCache = ignite.getOrCreateCache(CACHE_NAME);

        final ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();
        final List<Object> evts = Collections.synchronizedList(new ArrayList<>());

        qry.setLocalListener(new CacheEntryUpdatedListener<Object, Object>() {
            @Override public void onUpdated(
                Iterable<CacheEntryEvent<?, ?>> iterable) throws CacheEntryListenerException {
                for (CacheEntryEvent<?, ?> event : iterable)
                    evts.add(event);
            }
        });

        int iterations = 100;

        while (iterations-- > 0) {
            QueryCursor quorumCursor = managementCache.query(qry);

            IgniteInternalFuture<?> fut1 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    System.out.println("Deploy " + SERVICE_NAME);
                    deployService(ignite);

                    return null;
                }
            });

            IgniteInternalFuture<?> fut2 = GridTestUtils.runAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    System.out.println("Undeploy " + SERVICE_NAME);
                    ignite.services().cancel(SERVICE_NAME);

                    return null;
                }
            });

            fut1.get();
            fut2.get();

            U.sleep(100);

            assert evts.size() <= 1 : evts.size();

            ignite.services().cancel("service1");

            evts.clear();

            quorumCursor.close();
        }

    }

    /**
     * @param ignite Ignite.
     */
    private void deployService(final Ignite ignite) {
        ServiceConfiguration svcCfg = new ServiceConfiguration();

        svcCfg.setService(new ManagementService());
        svcCfg.setName(SERVICE_NAME);
        svcCfg.setTotalCount(1);
        svcCfg.setMaxPerNodeCount(1);
        svcCfg.setNodeFilter((IgnitePredicate<ClusterNode>)node -> !node.isClient());

        ignite.services().deploy(svcCfg);
    }

    /**
     *
     */
    public static class ManagementService implements Service {
        /** */
        private final String name = UUID.randomUUID().toString();

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            System.out.println(name + " shutdown.");
        }

        /** {@inheritDoc} */
        @Override public synchronized void init(ServiceContext ctx) throws Exception {
            System.out.println(name + " initializing.");

            ignite.cache(CACHE_NAME).put(TEST_KEY, name + " init");
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) throws Exception {
            // No-op
        }
    }
}
