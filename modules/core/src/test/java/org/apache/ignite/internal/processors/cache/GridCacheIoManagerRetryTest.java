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

package org.apache.ignite.internal.processors.cache;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.managers.systemview.GridSystemViewManager;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.deployment.local.LocalDeploymentSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.GridTestKernalContext;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.ignite.configuration.IgniteConfiguration.DFLT_SEND_RETRY_CNT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/** */
@RunWith(Parameterized.class)
public class GridCacheIoManagerRetryTest extends GridCommonAbstractTest {
    /** Remote node. */
    private static final ClusterNode REMOTE_NODE = new GridTestNode(UUID.randomUUID());

    /** Local node. */
    private static final ClusterNode LOCAL_NODE = new GridTestNode(UUID.randomUUID());

    /** Retry count. */
    @Parameter
    public int retryCnt;

    /** */
    @Parameters(name = "retryCnt={0}")
    public static Iterable<Integer> parameters() {
        return Arrays.asList(0, 1, DFLT_SEND_RETRY_CNT, 10);
    }

    /** */
    @Test
    public void testSend() throws Exception {
        // Only cluster node argument is useful for test.
        doTest(ctx -> () -> gridCacheIoManager(ctx)
                .send(REMOTE_NODE, new GridNearTxFinishResponse(), SYSTEM_POOL));
    }

    /** */
    @Test
    public void testSendOrdered() throws Exception {
        // Only cluster node argument is useful for test.
        doTest(ctx -> () -> gridCacheIoManager(ctx)
            .sendOrderedMessage(
                REMOTE_NODE,
                TOPIC_CACHE,
                new GridNearTxFinishResponse(),
                SYSTEM_POOL,
                Long.MAX_VALUE
            ));
    }

    /** */
    private void doTest(Function<GridKernalContext, RunnableX> action) throws Exception {
        AtomicInteger sendCnt = new AtomicInteger();

        GridKernalContext ctx = kernalContext(retryCnt, sendCnt);

        Throwable actionRes = assertThrows(log(), action.apply(ctx), IgniteException.class, "Test cause");

        // RunnableX rethrows IgniteCheckedException as IgniteException.
        assertTrue(X.hasCause(actionRes, IgniteCheckedException.class));

        assertEquals("Unexpected send count", retryCnt + 1 /* first send + retries */,
            sendCnt.get());
    }

    /**
     * Initialize GridCacheIoManager and GridCacheSharedContext.
     *
     * @param ctx Kernal context.
     */
    @SuppressWarnings("unchecked")
    @NotNull private static GridCacheIoManager gridCacheIoManager(GridKernalContext ctx) throws IgniteCheckedException {
        GridCacheIoManager cacheIoMgr = new GridCacheIoManager();

        GridCacheSharedContext<?, ?> cctx = GridCacheSharedContext.builder()
            .setIoManager(cacheIoMgr)
            .setPartitionExchangeManager(new GridCachePartitionExchangeManager<>())
            .build(ctx, null);

        cacheIoMgr.start(cctx);

        return cacheIoMgr;
    }

    /**
     * Configure and initalize GridKernalContext.
     *
     * @param retryCnt Reconnect count.
     * @param sendCnt Send attempts counter.
     */
    private GridKernalContext kernalContext(int retryCnt, AtomicInteger sendCnt) throws IgniteCheckedException {
        GridTestKernalContext ctx = newContext();

        // Necessary to init GridKernalContext.
        ctx.config().setPeerClassLoadingEnabled(true);
        ctx.config().setDeploymentSpi(new LocalDeploymentSpi());
        ctx.config().setCommunicationSpi(new TcpCommunicationSpi());

        ctx.config().setNetworkSendRetryCount(retryCnt);
        ctx.config().setNetworkSendRetryDelay(1);

        // Discovery returns remote and local nodes and successfully pings remote node.
        ctx.config().setDiscoverySpi(new TcpDiscoverySpi() {
            @Override public @Nullable ClusterNode getNode(UUID nodeId) {
                return nodeId.equals(REMOTE_NODE.id()) ? REMOTE_NODE :
                    nodeId.equals(LOCAL_NODE.id()) ? LOCAL_NODE : null;
            }

            @Override public boolean pingNode(UUID nodeId) {
                return nodeId.equals(REMOTE_NODE.id());
            }

            @Override public ClusterNode getLocalNode() {
                return LOCAL_NODE;
            }
        });

        // Necessary to init GridKernalContext.
        ctx.add(new PoolProcessor(ctx));
        ctx.add(new GridSystemViewManager(ctx));
        ctx.add(new IgnitePluginProcessor(ctx, ctx.config(), Collections.emptyList()));
        ctx.add(new GridDeploymentManager(ctx));

        ctx.add(new GridDiscoveryManager(ctx) {
            @Override public @Nullable ClusterNode node(UUID nodeId) {
                return nodeId.equals(REMOTE_NODE.id()) ? REMOTE_NODE :
                    nodeId.equals(LOCAL_NODE.id()) ? LOCAL_NODE : null;
            }
        });

        configureGridIoManager(ctx, sendCnt);

        return ctx;
    }

    /**
     * Configure GridIoManager, which throws exception and counts sending attempts.
     *
     * @param ctx Context.
     * @param sendCnt Send attempts counter.
     */
    private void configureGridIoManager(GridTestKernalContext ctx, AtomicInteger sendCnt) {
        ctx.add(new GridIoManager(ctx) {
            @Override public void sendToGridTopic(ClusterNode node, GridTopic topic, Message msg, byte plc)
                throws IgniteCheckedException {
                sendCnt.incrementAndGet();

                throw new IgniteCheckedException("Test cause");
            }

            @Override public void sendOrderedMessage(ClusterNode node, Object topic, Message msg, byte plc,
                long timeout, boolean skipOnTimeout) throws IgniteCheckedException {
                sendCnt.incrementAndGet();

                throw new IgniteCheckedException("Test cause");
            }
        });
    }
}
