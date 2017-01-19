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
package org.apache.ignite.internal.processors.cache.binary;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import junit.framework.AssertionFailedError;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class GridCacheBinaryObjectMetadataExchangeMultinodeTest extends GridCommonAbstractTest {
    /** */
    protected static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private boolean clientMode;

    /** */
    private boolean applyDiscoveryHook;

    /** */
    private DiscoveryHook discoveryHook;

    /** */
    private static final String BINARY_TYPE_NAME = "TestBinaryType";

    /** */
    private static final int BINARY_TYPE_ID = 708045005;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        if (applyDiscoveryHook) {
            final DiscoveryHook hook = discoveryHook != null ? discoveryHook : new DiscoveryHook();

            cfg.setDiscoverySpi(new TcpDiscoverySpi() {
                @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
                    super.setListener(DiscoverySpiListenerWrapper.wrap(lsnr, hook));
                }
            });
        }

        cfg.setPeerClassLoadingEnabled(false);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder).setForceServerMode(true);

        cfg.setMarshaller(new BinaryMarshaller());

        cfg.setClientMode(clientMode);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * Hook object intervenes to handling discovery message
     * and thus allows to make assertions or other actions like skipping certain discovery messages.
     */
    private static class DiscoveryHook {
        /**
         * @param msg Message.
         */
        boolean handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
            return true;
        }

        /**
         * @param ignite Ignite.
         */
        void ignite(IgniteEx ignite) {
            // No-op.
        }
    }

    /**
     * Injects {@link DiscoveryHook} into handling logic.
     */
    private static final class DiscoverySpiListenerWrapper implements DiscoverySpiListener {
        /** */
        private final DiscoverySpiListener delegate;

        /** */
        private final DiscoveryHook hook;

        /**
         * @param delegate Delegate.
         * @param hook Hook.
         */
        private DiscoverySpiListenerWrapper(DiscoverySpiListener delegate, DiscoveryHook hook) {
            this.hook = hook;
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override public void onDiscovery(int type, long topVer, ClusterNode node, Collection<ClusterNode> topSnapshot, @Nullable Map<Long, Collection<ClusterNode>> topHist, @Nullable DiscoverySpiCustomMessage spiCustomMsg) {
            if (hook.handleDiscoveryMessage(spiCustomMsg))
                delegate.onDiscovery(type, topVer, node, topSnapshot, topHist, spiCustomMsg);
        }

        /**
         * @param delegate Delegate.
         * @param discoveryHook Discovery hook.
         */
        static DiscoverySpiListener wrap(DiscoverySpiListener delegate, DiscoveryHook discoveryHook) {
            return new DiscoverySpiListenerWrapper(delegate, discoveryHook);
        }
    }

    /**
     *
     */
    private static final class ErrorHolder {
        /** */
        private volatile Error e;

        /**
         * @param e Exception.
         */
        void error(Error e) {
            this.e = e;
        }

        /**
         *
         */
        void fail() {
            throw e;
        }

        /**
         *
         */
        boolean isEmpty() {
            return e == null;
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** */
    private static final CountDownLatch LATCH1 = new CountDownLatch(1);

    /**
     * Verifies that if thread tries to read metadata with ongoing update it gets blocked
     * until acknowledge message arrives.
     */
    public void testReadRequestBlockedOnUpdatingMetadata() throws Exception {
        applyDiscoveryHook = true;
        discoveryHook = new DiscoveryHook() {
            @Override public boolean handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ? null
                        : (DiscoveryCustomMessage) IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataUpdateAcceptedMessage) {
                    if (((MetadataUpdateAcceptedMessage)customMsg).typeId() == BINARY_TYPE_ID)
                        try {
                            Thread.sleep(300);
                        }
                        catch (InterruptedException ignored) {
                            // No-op.
                        }
                }

                return true;
            }
        };

        final IgniteEx ignite0 = startGrid(0);

        applyDiscoveryHook = false;

        final IgniteEx ignite1 = startGrid(1);

        final ErrorHolder errorHolder = new ErrorHolder();

        applyDiscoveryHook = true;
        discoveryHook = new DiscoveryHook() {
            private volatile IgniteEx ignite;

            @Override public boolean handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ? null
                        : (DiscoveryCustomMessage) IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataUpdateAcceptedMessage) {
                    MetadataUpdateAcceptedMessage acceptedMsg = (MetadataUpdateAcceptedMessage)customMsg;
                    if (acceptedMsg.typeId() == BINARY_TYPE_ID && acceptedMsg.acceptedVersion() == 2) {
                        Object binaryProc = U.field(ignite.context(), "cacheObjProc");
                        Object transport = U.field(binaryProc, "transport");

                        try {
                            Map syncMap = U.field(transport, "syncMap");

                            int size = syncMap.size();
                            assertEquals("unexpected size of syncMap: ", 1, size);

                            Object syncKey = syncMap.keySet().iterator().next();

                            int typeId = U.field(syncKey, "typeId");
                            assertEquals("unexpected typeId: ", BINARY_TYPE_ID, typeId);

                            int ver = U.field(syncKey, "ver");
                            assertEquals("unexpected pendingVersion: ", 2, ver);
                        }
                        catch (AssertionFailedError err) {
                            errorHolder.error(err);
                        }
                    }
                }

                return true;
            }

            @Override public void ignite(IgniteEx ignite) {
                this.ignite = ignite;
            }
        };

        final IgniteEx ignite2 = startGrid(2);
        discoveryHook.ignite(ignite2);

        ignite0.executorService().submit(new Runnable() {
            @Override public void run() {
                addIntField(ignite0, "f1", 101, 1);
            }
        }).get();

        UUID id2 = ignite2.localNode().id();

        ClusterGroup cg2 = ignite2.cluster().forNodeId(id2);

        Future<?> fut = ignite1.executorService().submit(new Runnable() {
            @Override public void run() {
                LATCH1.countDown();
                addStringField(ignite1, "f2", "str", 2);
            }
        });

        ignite2.compute(cg2).withAsync().call(new IgniteCallable<Object>() {
            @Override public Object call() throws Exception {
                try {
                    LATCH1.await();
                }
                catch (InterruptedException ignored) {
                    // No-op.
                }

                Object fieldVal = ((BinaryObject) ignite2.cache(null).withKeepBinary().get(1)).field("f1");

                return fieldVal;
            }
        });

        fut.get();

        if (!errorHolder.isEmpty())
            errorHolder.fail();
    }

    /**
     * Verifies that all sequential updates that don't introduce any conflicts are accepted and observed by all nodes.
     */
    public void testSequentialUpdatesNoConflicts() throws Exception {
        IgniteEx ignite0 = startGrid(0);

        final IgniteEx ignite1 = startGrid(1);

        final String intFieldName = "f1";

        ignite1.executorService().submit(new Runnable() {
            @Override public void run() {
                addIntField(ignite1, intFieldName, 101, 1);
            }
        }).get();

        assertEquals(((BinaryObject)ignite0.cache(null).withKeepBinary().get(1)).field(intFieldName), 101);

        final IgniteEx ignite2 = startGrid(2);

        final String strFieldName = "f2";

        ignite2.executorService().submit(new Runnable() {
            @Override public void run() {
                addStringField(ignite2, strFieldName, "str", 2);
            }
        }).get();

        assertEquals(((BinaryObject)ignite1.cache(null).withKeepBinary().get(2)).field(strFieldName), "str");
    }

    /**
     * Verifies that client is able to detect obsolete metadata situation and request up-to-date from the cluster.
     */
    public void testClientRequestsUpToDateMetadata() throws Exception {
        final IgniteEx ignite0 = startGrid(0);

        final IgniteEx ignite1 = startGrid(1);

        ignite0.executorService().submit(new Runnable() {
            @Override public void run() {
                addIntField(ignite0, "f1", 101, 1);
            }
        }).get();

        clientMode = true;
        applyDiscoveryHook = true;
        discoveryHook = new DiscoveryHook() {
            @Override boolean handleDiscoveryMessage(DiscoverySpiCustomMessage msg) {
                DiscoveryCustomMessage customMsg = msg == null ? null
                        : (DiscoveryCustomMessage) IgniteUtils.field(msg, "delegate");

                if (customMsg instanceof MetadataUpdateProposedMessage) {
                    if (((MetadataUpdateProposedMessage) customMsg).typeId() == BINARY_TYPE_ID)
                        return false;
                }
                else if (customMsg instanceof MetadataUpdateAcceptedMessage) {
                    if (((MetadataUpdateAcceptedMessage) customMsg).typeId() == BINARY_TYPE_ID)
                        return false;
                }

                return true;
            }
        };

        final Ignite client = startGrid("client");

        ClusterGroup clientGrp = client.cluster().forClients();

        final String strVal = "strVal101";

        ignite1.executorService().submit(new Runnable() {
            @Override public void run() {
                addStringField(ignite1, "f2", strVal, 1);
            }
        }).get();

        String res = client.compute(clientGrp).call(new IgniteCallable<String>() {
            @Override public String call() throws Exception {
                return ((BinaryObject)client.cache(null).withKeepBinary().get(1)).field("f2");
            }
        });

        assertEquals(strVal, res);
    }

    /**
     * Adds field of integer type to fixed binary type.
     *
     * @param ignite Ignite.
     * @param fieldName Field name.
     * @param fieldVal Field value.
     * @param cacheIdx Cache index.
     */
    private void addIntField(Ignite ignite, String fieldName, int fieldVal, int cacheIdx) {
        BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

        IgniteCache<Object, Object> cache = ignite.cache(null).withKeepBinary();

        builder.setField(fieldName, fieldVal);

        cache.put(cacheIdx, builder.build());
    }

    /**
     * Adds field of String type to fixed binary type.
     *
     * @param ignite Ignite.
     * @param fieldName Field name.
     * @param fieldVal Field value.
     * @param cacheIdx Cache index.
     */
    private void addStringField(Ignite ignite, String fieldName, String fieldVal, int cacheIdx) {
        BinaryObjectBuilder builder = ignite.binary().builder(BINARY_TYPE_NAME);

        IgniteCache<Object, Object> cache = ignite.cache(null).withKeepBinary();

        builder.setField(fieldName, fieldVal);

        cache.put(cacheIdx, builder.build());
    }
}
