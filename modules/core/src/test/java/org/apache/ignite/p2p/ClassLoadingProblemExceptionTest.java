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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.deployment.GridDeploymentManager;
import org.apache.ignite.internal.managers.deployment.GridDeploymentMetadata;
import org.apache.ignite.internal.managers.deployment.GridDeploymentStore;
import org.apache.ignite.internal.managers.deployment.GridTestDeployment;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.internal.processors.cache.IgnitePeerToPeerClassLoadingException;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 *
 */
public class ClassLoadingProblemExceptionTest extends GridCommonAbstractTest implements Serializable {
    /** Test predicate class name. */
    private static final String PREDICATE_NAME = "org.apache.ignite.tests.p2p.P2PTestPredicate";

    /** Flag that allows to test the check on receiving message. */
    private boolean spoilMsgOnSnd = false;

    /** Flag that allows to test the check while preparing message on sender. */
    private boolean spoilDeploymentBeforePrepare = false;

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    private static final String CLIENT_PREFIX = "client";

    /** */
    private static final String CLIENT_1 = CLIENT_PREFIX + "1";

    /** */
    private static final String CLIENT_2 = CLIENT_PREFIX + "2";

    /** Container for exception that can be thrown inside of job. */
    private static AtomicReference<Throwable> exceptionThrown = new AtomicReference<>(null);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg
            .setClientMode(igniteInstanceName.startsWith(CLIENT_PREFIX))
            .setPeerClassLoadingEnabled(true)
            .setDeploymentMode(SHARED)
            .setCommunicationSpi(new TestCommunicationSpi())
            .setMarshaller(new OptimizedMarshaller());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     *
     * @param spoilDeploymentBeforePrepare Test the check on preparing message.
     * @param spoilMsgOnSnd Test the check on receiving message.
     * @param eExp Exception expected.
     * @throws Exception If failed.
     */
    private void doTest(
        boolean spoilDeploymentBeforePrepare,
        boolean spoilMsgOnSnd,
        Class<? extends Throwable> eExp
    ) throws Exception {
        this.spoilDeploymentBeforePrepare = spoilDeploymentBeforePrepare;
        this.spoilMsgOnSnd = spoilMsgOnSnd;
        this.exceptionThrown.set(null);

        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(1);

        crd.getOrCreateCache(CACHE_NAME);

        IgniteEx client1 = startGrid(CLIENT_1);
        IgniteEx client2 = startGrid(CLIENT_2);

        awaitPartitionMapExchange();

        GridDeploymentManager deploymentMgr = client2.context().deploy();

        GridDeploymentStore store = GridTestUtils.getFieldValue(deploymentMgr, "locStore");

        GridTestUtils.setFieldValue(deploymentMgr, "locStore", new GridDeploymentTestStore(store));

        client1.compute(client1.cluster().forRemotes()).execute(new P2PDeploymentLongRunningTask(), null);

        if (exceptionThrown.get() != null)
            exceptionThrown.get().printStackTrace();

        assertTrue(
            "Wrong exception: " + exceptionThrown.get(),
            exceptionThrown.get() == null && eExp == null || X.hasCause(exceptionThrown.get(), eExp)
        );
    }

    /** */
    @Test
    public void testDontSpoil() throws Exception {
        doTest(false, false, null);
    }

    /** */
    @Test
    public void testSpoilBeforePrepare() throws Exception {
        doTest(true, false, IgnitePeerToPeerClassLoadingException.class);
    }

    /** */
    @Test
    public void testSpoilOnSend() throws Exception {
        doTest(false, true, IgnitePeerToPeerClassLoadingException.class);
    }

    /** */
    @Test
    public void testSpoilBoth() throws Exception {
        doTest(true, true, IgnitePeerToPeerClassLoadingException.class);
    }

    /**
     *
     */
    private class TestCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (spoilMsgOnSnd && msg instanceof GridIoMessage) {
                GridIoMessage ioMsg = (GridIoMessage)msg;

                Message m = ioMsg.message();

                if (m instanceof GridCacheQueryRequest) {
                    GridCacheQueryRequest queryRequest = (GridCacheQueryRequest)m;

                    if (queryRequest.deployInfo() != null) {
                        queryRequest.prepare(
                            new GridDeploymentInfoBean(
                                IgniteUuid.fromUuid(UUID.randomUUID()),
                                queryRequest.deployInfo().userVersion(),
                                queryRequest.deployInfo().deployMode(),
                                null
                            )
                        );
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }

    /**
     *
     */
    private class GridDeploymentTestStore implements GridDeploymentStore {
        /** */
        private final GridDeploymentStore store;

        /** */
        public GridDeploymentTestStore(GridDeploymentStore store) {
            this.store = store;
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            store.start();
        }

        /** {@inheritDoc} */
        @Override public void stop() {
            store.stop();
        }

        /** {@inheritDoc} */
        @Override public void onKernalStart() throws IgniteCheckedException {
            store.onKernalStart();
        }

        /** {@inheritDoc} */
        @Override public void onKernalStop() {
            store.onKernalStop();
        }

        /** {@inheritDoc} */
        @Override public @Nullable GridDeployment getDeployment(GridDeploymentMetadata meta) {
            return store.getDeployment(meta);
        }

        /** {@inheritDoc} */
        @Override public @Nullable GridDeployment searchDeploymentCache(GridDeploymentMetadata meta) {
            return store.searchDeploymentCache(meta);
        }

        /** {@inheritDoc} */
        @Override public @Nullable GridDeployment getDeployment(IgniteUuid ldrId) {
            return store.getDeployment(ldrId);
        }

        /** {@inheritDoc} */
        @Override public Collection<GridDeployment> getDeployments() {
            return store.getDeployments();
        }

        /** {@inheritDoc} */
        @Override public GridDeployment explicitDeploy(Class<?> cls, ClassLoader clsLdr) throws IgniteCheckedException {
            if (spoilDeploymentBeforePrepare) {
                IgniteUuid ldrId = IgniteUuid.fromUuid(UUID.randomUUID());

                return new GridTestDeployment(SHARED, getClass().getClassLoader(), ldrId, "0", PREDICATE_NAME, false);
            }
            else
                return store.explicitDeploy(cls, clsLdr);
        }

        /** {@inheritDoc} */
        @Override public void explicitUndeploy(@Nullable UUID nodeId, String rsrcName) {
            store.explicitUndeploy(nodeId, rsrcName);
        }

        /** {@inheritDoc} */
        @Override public void addParticipants(Map<UUID, IgniteUuid> allParticipants, Map<UUID, IgniteUuid> addedParticipants) {
            store.addParticipants(allParticipants, addedParticipants);
        }
    }

    /**
     *
     */
    public static class P2PDeploymentLongRunningTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteException {
            IntFunction<ComputeJobAdapter> f = i -> new ComputeJobAdapter() {
                /** */
                @IgniteInstanceResource
                private transient IgniteEx ignite;

                /** {@inheritDoc} */
                @Override public Object execute() {
                    try {
                        if (ignite.configuration().getIgniteInstanceName().equals(CLIENT_2)) {
                            IgniteCache<Integer, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

                            Class<IgniteBiPredicate> cls = (Class<IgniteBiPredicate>)getExternalClassLoader().loadClass(PREDICATE_NAME);

                            cache.query(new ScanQuery<IgniteBiPredicate, Integer>(cls.newInstance())).getAll();
                        }
                    }
                    catch (Throwable e) {
                        exceptionThrown.set(e);
                    }

                    return null;
                }
            };

            return IntStream.range(0, gridSize).mapToObj(f).collect(Collectors.toList());
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }
}
