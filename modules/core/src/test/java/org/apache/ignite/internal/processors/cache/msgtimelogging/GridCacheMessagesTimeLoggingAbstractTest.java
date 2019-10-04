/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.msgtimelogging;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.LongStream;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxQueryFirstEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicDeferredUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxEnlistResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.metric.impl.HistogramMetric;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.TimeLoggableRequest;
import org.apache.ignite.plugin.extensions.communication.TimeLoggableResponse;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpiMBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Class containing utility methods for testing messages network time.
 */
public abstract class GridCacheMessagesTimeLoggingAbstractTest extends GridCommonAbstractTest {
    /** Grid count. */
    protected static final int GRID_CNT = 3;

    /**
     * Stores response classes as keys and lists of request classes that trigger responses as value.
     * Stores only those request classes that trigger response obligatory.
     */
    private static final Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> reqRespMappingStrict =
        initReqRespMappingStrict();

    /**
     * Stores response classes as keys and lists of request classes that trigger responses as value.
     * Stores request classes that trigger responses.
     */
    private static final Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> reqRespMappingNonStrict =
        initReqRespMappingNonStrict();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(TRANSACTIONAL);

        ccfg.setBackups(2);

        cfg.setCacheConfiguration(ccfg);

        cfg.setCommunicationSpi(new RecordingSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param sourceNodeIdx Index of node that stores metric.
     * @param targetNodeIdx Index of node where requests are sent.
     * @param respCls Metric request class.
     * @return {@code HistogramMetric} for {@code respCls}.
     */
    @Nullable public HistogramMetric getMetric(
        int sourceNodeIdx,
        int targetNodeIdx,
        Class respCls
    ) {
        return getMetric(grid(sourceNodeIdx).name(), grid(targetNodeIdx).localNode().id(), respCls);
    }

    /** */
    public static HistogramMetric getMetric (
        ClusterNode srcNode,
        ClusterNode targetNode,
        Class respCls
    ) {
        return getMetric(G.ignite(srcNode.id()).name(), targetNode.id(), respCls);
    }

    /**
     * @param srcInstanceName Source node instance name.
     * @param targetNodeId Id of node where requests are sent.
     * @param respCls Metric request class.
     * @return {@code HistogramMetric} for {@code respCls}.
     */
    @Nullable public static HistogramMetric getMetric(
        String srcInstanceName,
        UUID targetNodeId,
        Class respCls
    ) {
        TcpCommunicationSpiMBean mbean = mbean(srcInstanceName);

        if (mbean == null)
            return null;

        Map<UUID, Map<String, HistogramMetric>> nodeMap = mbean.getOutMetricsByNodeByMsgClass();

        assertNotNull(nodeMap);

        Map<String, HistogramMetric> clsNameMap = nodeMap.get(targetNodeId);

        if (clsNameMap == null)
            return null;

        return clsNameMap.get(respCls.getName());
    }

    /** */
    public TcpCommunicationSpiMBean mbean(int nodeIdx) {
        return mbean(getTestIgniteInstanceName(nodeIdx));
    }

    /**
     * Gets TcpCommunicationSpiMBean by name.
     *
     * @param igniteInstanceName mbean name.
     * @return MBean instance.
     */
    public static TcpCommunicationSpiMBean mbean(String igniteInstanceName) {
        ObjectName mbeanName;

        try {
            mbeanName = U.makeMBeanName(igniteInstanceName, "SPIs", RecordingSpi.class.getSimpleName());
        }
        catch (MalformedObjectNameException e) {
            fail("Failed to construct mbean name");
            return null;
        }

        MBeanServer mbeanSrv = ManagementFactory.getPlatformMBeanServer();

        if (mbeanSrv.isRegistered(mbeanName))
            return MBeanServerInvocationHandler.newProxyInstance(mbeanSrv, mbeanName, TcpCommunicationSpiMBean.class,
                true);
        else
            fail("MBean is not registered: " + mbeanName.getCanonicalName());

        return null;
    }

    /** */
    public static void populateCache(IgniteCache<Integer, Integer> cache) {
        Map<Integer, Integer> entriesToAdd = new HashMap<>();
        Set<Integer> keysToRemove = new HashSet<>();
        Set<Integer> keysToGet = new HashSet<>();

        for (int i = 0; i < 20; i++) {
            cache.put(i, i);
            entriesToAdd.put(i + 20, i * 2);
        }

        cache.putAll(entriesToAdd);

        for (int i = 0; i < 10; i++) {
            cache.remove(i);
            keysToRemove.add(i + 20);
        }

        cache.removeAll(keysToRemove);

        for (int i = 0; i < 10; i++) {
            cache.get(i + 5);
            keysToGet.add(i);
        }

        cache.getAll(keysToGet);

        cache.size();
    }

    /**
     * Checks time loggable messages and histograms consistency for all pairs of nodes.
     */
    public static void checkTimeLoggableMsgsConsistancy() {
        List<Ignite> allGrids = G.allGrids();
        assertFalse("No nodes in cluster", allGrids.isEmpty());

        Ignite ignite = allGrids.get(0);

        List<ClusterNode> allNodes = new ArrayList<>(ignite.cluster().nodes());

        for (int i = 0; i < allNodes.size(); i++) {
            for (int j = 0; j < allNodes.size(); j++) {
                if (i == j)
                    continue;

                checkTimeLoggableMsgsConsistancy(allNodes.get(i), allNodes.get(j));
            }
        }
    }

    /**
     * Checks that number TimeLoggableResponses sent from {@code targetNode} equals to
     * number of TimeLoggableRequests sent from {@code srcNode}. Numbers of messages is
     * compared for every class.
     *
     * @param srcNode Source node.
     * @param targetNode Target node.
     */
    private static void checkTimeLoggableMsgsConsistancy(ClusterNode srcNode, ClusterNode targetNode) {
        RecordingSpi srcSpi = (RecordingSpi)G.ignite(srcNode.id()).configuration().getCommunicationSpi();
        RecordingSpi targetSpi = (RecordingSpi)G.ignite(targetNode.id()).configuration().getCommunicationSpi();

        Map<Class<? extends TimeLoggableResponse>, Integer> sentFromTarget = targetSpi.respClsMap.get(srcNode.id());

        // No TimeLoggableResponse received.
        if (sentFromTarget == null)
            return;

        Map<Class<? extends TimeLoggableRequest>, Integer> sentFromSource = srcSpi.reqClsMap.get(targetNode.id());

        // If there were responses there must be requests.
        assertNotNull(sentFromSource);

        // Number of received responses must be less or equal to number of requests that could trigger this response.
        sentFromTarget.forEach((cls, respNum) -> {
            List<Class<? extends TimeLoggableRequest>> reqClasses = reqRespMappingStrict.get(cls);

            boolean strictCheck = true;

            if (reqClasses == null) {
                reqClasses = reqRespMappingNonStrict.get(cls);

                assertNotNull("Class not mapped: " + cls, reqClasses);

                strictCheck = false;
            }

            int totalReqsNum = 0;
            for (Class<? extends TimeLoggableRequest> reqCls: reqClasses) {
                Integer reqsNum = sentFromSource.get(reqCls);

                if (reqsNum == null)
                    continue;

                totalReqsNum += reqsNum;
            }

            String errorMsg = "Too many TimeLoggableResponses. totalReqsNum: " + totalReqsNum + "; respNum: " +
                              respNum + "; resp class: " + cls;

            System.out.println("checking num for " + cls);

            if (strictCheck) {
                assertEquals(errorMsg, totalReqsNum, (int)respNum);

                if (respNum != 0) {
                    HistogramMetric metric = getMetric(srcNode, targetNode, cls);

                    assertNotNull(metric);

                    System.out.println("validating metric for class " + cls);

                    assertEquals(LongStream.of(metric.value()).sum(), (long)respNum);
                }
            }
            else
                assertTrue(errorMsg, totalReqsNum >= respNum);
        });
    }

    /**
     * Counts sent messages num per message class.
     */
    public static class RecordingSpi extends TcpCommunicationSpi {
        /** */
        Map<UUID, Map<Class<? extends TimeLoggableRequest>, Integer>> reqClsMap = new ConcurrentHashMap<>();

        /** */
        Map<UUID, Map<Class<? extends TimeLoggableResponse>, Integer>> respClsMap = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            recordMessage(node, msg);

            super.sendMessage(node, msg);
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            recordMessage(node, msg);

            super.sendMessage(node, msg, ackC);
        }

        /** */
        private void recordMessage(ClusterNode node, Message msg) {
            if (!node.isLocal()) {
                Message msg0 = msg;

                if (msg instanceof GridIoMessage)
                    msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof TimeLoggableRequest) {
                    Map<Class<? extends TimeLoggableRequest>, Integer> nodeMap =
                        reqClsMap.computeIfAbsent(node.id(), k -> new ConcurrentHashMap<>());

                    nodeMap.merge(((TimeLoggableRequest)msg0).getClass(), 1, Integer::sum);
                } else if (msg0 instanceof TimeLoggableResponse) {
                    Map<Class<? extends TimeLoggableResponse>, Integer> nodeMap =
                        respClsMap.computeIfAbsent(node.id(), k -> new ConcurrentHashMap<>());

                    nodeMap.merge(((TimeLoggableResponse)msg0).getClass(), 1, Integer::sum);
                }
            }
        }
    }

    /**
     * Custom metrics listener that effectively increases messages network time
     * for {@code TimeLoggableResponse}
     */
    protected static class SleepingMetricsListener extends TcpCommunicationMetricsListener {
        /** */
        private final int sleepTime;

        /** */
        public SleepingMetricsListener(int sleepTime) {
            this.sleepTime = sleepTime;
        }

        /** {@inheritDoc} */
        @Override public void writeMessageSendTimestamp(Message msg) {
            super.writeMessageSendTimestamp(msg);

            if (msg instanceof GridIoMessage) {
                Message msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof TimeLoggableResponse)
                    doSleep(sleepTime);
            }
        }
    }

    /** */
    private static Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> initReqRespMappingNonStrict() {
        Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> res = new HashMap<>();

        res.putAll(reqRespMappingStrict);

        addReqRespMapping(res, GridDhtAtomicDeferredUpdateResponse.class, GridDhtAtomicSingleUpdateRequest.class,
            GridDhtAtomicUpdateRequest.class);

        return res;
    }

    /** */
    private static Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> initReqRespMappingStrict() {
        Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> res = new HashMap<>();

        addReqRespMapping(res, GridNearTxEnlistResponse.class, GridNearTxEnlistRequest.class);
        addReqRespMapping(res, GridNearTxPrepareResponse.class, GridNearTxPrepareRequest.class);
        addReqRespMapping(res, GridDhtTxQueryEnlistResponse.class, GridDhtTxQueryFirstEnlistRequest.class);
        addReqRespMapping(res, GridDhtTxPrepareResponse.class, GridDhtTxPrepareRequest.class);
        addReqRespMapping(res, GridNearSingleGetResponse.class, GridNearSingleGetRequest.class);
        addReqRespMapping(res, GridNearGetResponse.class, GridNearGetRequest.class);
        addReqRespMapping(res, GridNearLockResponse.class, GridNearLockRequest.class);
        addReqRespMapping(res, GridDhtTxFinishResponse.class, GridDhtTxFinishRequest.class);

        addReqRespMapping(res, GridNearAtomicUpdateResponse.class, GridNearAtomicFullUpdateRequest.class,
           GridNearAtomicSingleUpdateRequest.class, GridNearAtomicSingleUpdateInvokeRequest.class,
           GridNearAtomicSingleUpdateFilterRequest.class);

        return res;
    }

    /** */
    private static void addReqRespMapping (
        Map<Class<? extends TimeLoggableResponse>, List<Class<? extends TimeLoggableRequest>>> map,
        Class<? extends TimeLoggableResponse> resp,
        Class<? extends TimeLoggableRequest> ... reqs)
    {
        List<Class<? extends TimeLoggableRequest>> reqList = new ArrayList<>(Arrays.asList(reqs));

        map.put(resp, reqList);
    }
}
