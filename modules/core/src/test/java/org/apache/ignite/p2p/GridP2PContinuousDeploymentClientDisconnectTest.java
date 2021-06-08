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

package org.apache.ignite.p2p;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.CacheException;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.query.AbstractContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ContinuousQueryWithTransformer;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentRequest;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.GridTopic.TOPIC_CLASSLOAD;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Tests for client disconnection during continuous query deployment.
 */
public class GridP2PContinuousDeploymentClientDisconnectTest extends GridCommonAbstractTest {
    /** */
    private ListeningTestLogger testLog;

    /** The resource name which is used in static initializer block. */
    private static final String P2P_TEST_OBJ_RSRC_NAME =
        "org/apache/ignite/tests/p2p/GridP2PTestObjectWithStaticInitializer$GridP2PTestObject.class";

    /** */
    private static final String REMOTE_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.GridP2PSerializableRemoteFilterWithStaticInitializer";

    /** */
    private static final String REMOTE_FILTER_FACTORY_CLS_NAME =
        "org.apache.ignite.tests.p2p.GridP2PRemoteFilterWithStaticInitializerFactory";

    /** */
    private static final String REMOTE_TRANSFORMER_FACTORY_CLS_NAME =
        "org.apache.ignite.tests.p2p.GridP2PRemoteTransformerWithStaticInitializerFactory";

    /** */
    private static final String EVT_REMOTE_FILTER_CLS_NAME = "org.apache.ignite.tests.p2p.GridP2PEventRemoteFilterWithStaticInitializer";

    /** */
    private static final String MSG_REMOTE_LSNR_CLS_NAME = "org.apache.ignite.tests.p2p.GridP2PMessageRemoteListenerWithStaticInitializer";

    /** Flag that is raised in case the failure handler was called. */
    private final AtomicBoolean failure = new AtomicBoolean(false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(defaultCacheConfiguration());

        cfg.setGridLogger(testLog);

        cfg.setFailureHandler(new AbstractFailureHandler() {
            /** {@inheritDoc} */
            @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                failure.set(true);

                return false;
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        testLog = new ListeningTestLogger(true, log);

        startGrid(0);

        startClientGrid(1);

        blockClassLoadingRequest(grid(1));
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        testLog.clearListeners();

        stopAllGrids();
    }

    /**
     * Test starts 1 server node and 1 client node. Class-loading request for the {@link #P2P_TEST_OBJ_RSRC_NAME}
     * resource blocks on the client node. The client node tries to deploy CQ with remote filter for
     * the cache {@link #DEFAULT_CACHE_NAME}.
     * Expected that exception with 'Failed to unmarshal deployable object.' error message will be thrown and
     * the server node wouldn't be failed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryRemoteFilter() throws Exception {
        final Class<CacheEntryEventSerializableFilter<Integer, Integer>> rmtFilterCls =
            (Class<CacheEntryEventSerializableFilter<Integer, Integer>>)getExternalClassLoader().
                loadClass(REMOTE_FILTER_CLS_NAME);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<Integer, Integer>()
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilter(rmtFilterCls.newInstance());

        IgniteEx client = grid(1);

        LogListener lsnr = LogListener.matches(
            "Failed to unmarshal deployable object."
        ).build();

        testLog.registerListener(lsnr);

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        assertThrowsWithCause(() -> cache.query(qry), CacheException.class);

        assertTrue(lsnr.check());

        // Check that the failure handler was not called.
        assertFalse(failure.get());
    }

    /**
     * Test starts 1 server node and 1 client node. Class-loading request for the {@link #P2P_TEST_OBJ_RSRC_NAME}
     * resource blocks on the client node. The client node tries to deploy CQ with remote filter factory for
     * the cache {@link #DEFAULT_CACHE_NAME}.
     * Expected that exception with 'Failed to initialize a continuous query.' error message will be thrown and
     * the server node wouldn't be failed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryRemoteFilterFactory() throws Exception {
        final Class<Factory<? extends CacheEntryEventFilter<Integer, Integer>>> rmtFilterFactoryCls =
            (Class<Factory<? extends CacheEntryEventFilter<Integer, Integer>>>)getExternalClassLoader().
                loadClass(REMOTE_FILTER_FACTORY_CLS_NAME);

        AbstractContinuousQuery<Integer, Integer> qry = new ContinuousQuery<Integer, Integer>()
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteFilterFactory(rmtFilterFactoryCls.newInstance());

        IgniteEx client = grid(1);

        LogListener lsnr = LogListener.matches(
            "Failed to initialize a continuous query."
        ).build();

        testLog.registerListener(lsnr);

        IgniteCache<Integer, Integer> cache = client.cache(DEFAULT_CACHE_NAME);

        cache.query(qry);

        assertTrue(lsnr.check());

        // Check that the failure handler was not called.
        assertFalse(failure.get());
    }

    /**
     * Test starts 1 server node and 1 client node. Class-loading request for the {@link #P2P_TEST_OBJ_RSRC_NAME}
     * resource blocks on the client node. The client node tries to deploy CQ with remote transformer for
     * the cache {@link #DEFAULT_CACHE_NAME}.
     * Expected that exception with 'Failed to unmarshal deployable object.' error message will be thrown and
     * the server node wouldn't be failed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testContinuousQueryRemoteTransformer() throws Exception {
        Class<Factory<IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, String>>> rmtTransformerFactoryCls =
            (Class<Factory<IgniteClosure<CacheEntryEvent<? extends Integer, ? extends Integer>, String>>>) getExternalClassLoader()
                .loadClass(REMOTE_TRANSFORMER_FACTORY_CLS_NAME);

        ContinuousQueryWithTransformer<Integer, Integer, String> qry = new ContinuousQueryWithTransformer<Integer, Integer, String>()
            .setLocalListener(evts -> {
                // No-op.
            })
            .setRemoteTransformerFactory(rmtTransformerFactoryCls.newInstance());

        LogListener lsnr = LogListener.matches(
            "Failed to initialize a continuous query."
        ).build();

        testLog.registerListener(lsnr);

        IgniteCache<Integer, Integer> cache = grid(1).cache(DEFAULT_CACHE_NAME);

        cache.query(qry);

        assertTrue(lsnr.check());

        // Check that the failure handler was not called.
        assertFalse(failure.get());
    }

    /**
     * Test starts 1 server node and 1 client node. Class-loading request for the {@link #P2P_TEST_OBJ_RSRC_NAME}
     * resource blocks on the client node. The client node tries to deploy remote event listener for
     * the cache {@link #DEFAULT_CACHE_NAME}.
     * Expected that exception with 'Failed to unmarshal deployable object.' error message will be thrown and
     * the server node wouldn't be failed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEventRemoteFilter() throws Exception {
        final Class<IgnitePredicate<Event>> evtFilterCls =
            (Class<IgnitePredicate<Event>>) getExternalClassLoader().loadClass(EVT_REMOTE_FILTER_CLS_NAME);

        LogListener lsnr = LogListener.matches(
            "Failed to unmarshal deployable object."
        ).build();

        testLog.registerListener(lsnr);

        assertThrowsWithCause(
            () -> grid(1)
                .events()
                .remoteListen(
                    (uuid, event) -> true,
                    evtFilterCls.newInstance(),
                    EventType.EVT_NODE_JOINED
                ),
            IgniteException.class
        );

        assertTrue(lsnr.check());

        // Check that the failure handler was not called.
        assertFalse(failure.get());
    }

    /**
     * Test starts 1 server node and 1 client node. Class-loading request for the {@link #P2P_TEST_OBJ_RSRC_NAME}
     * resource blocks on the client node. The client node tries to deploy remote message listener for
     * the cache {@link #DEFAULT_CACHE_NAME}.
     * Expected that exception with 'Failed to unmarshal deployable object.' error message will be thrown and
     * the server node wouldn't be failed.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testMessageRemoteListen() throws Exception {
        Class<IgniteBiPredicate<UUID, String>> rmtLsnrCls =
            (Class<IgniteBiPredicate<UUID, String>>) getExternalClassLoader().loadClass(MSG_REMOTE_LSNR_CLS_NAME);

        LogListener lsnr = LogListener.matches(
            "Failed to unmarshal deployable object."
        ).build();

        testLog.registerListener(lsnr);

        assertThrowsWithCause(
            () -> grid(1)
                .message()
                .remoteListen("test", rmtLsnrCls.newInstance()),
            IgniteException.class
        );

        assertTrue(lsnr.check());

        // Check that the failure handler was not called.
        assertFalse(failure.get());
    }

    /**
     * Blocks peer-class loading for {@link #P2P_TEST_OBJ_RSRC_NAME} resource.
     *
     * @param node The node where peer-class loading should be blocked.
     */
    private void blockClassLoadingRequest(IgniteEx node) {
        GridKernalContext ctx = node.context();

        GridDeploymentManager deploymentMgr = ctx.deploy();

        Object comm = GridTestUtils.getFieldValue(deploymentMgr, "comm");

        GridMessageListener peerLsnr = GridTestUtils.getFieldValue(comm, "peerLsnr");

        ctx.io().removeMessageListener(TOPIC_CLASSLOAD, peerLsnr);

        GridMessageListener newPeerLsnr = new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                GridDeploymentRequest req = (GridDeploymentRequest)msg;

                String rsrcName = GridTestUtils.getFieldValue(req, "rsrcName");

                if (rsrcName.equals(P2P_TEST_OBJ_RSRC_NAME))
                    return;

                peerLsnr.onMessage(nodeId, msg, plc);
            }
        };

        ctx.io().addMessageListener(TOPIC_CLASSLOAD, newPeerLsnr);
    }
}
