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

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.DiscoverySpiCustomMessage;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class IgniteMarshallerCacheFSRestoreTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean isDuplicateObserved = true;

    /**
     *
     */
    private static class SimpleValue {
        /** */
        private final int iF;

        /** */
        private final String sF;

        /**
         * @param iF Int field.
         * @param sF String field.
         */
        SimpleValue(int iF, String sF) {
            this.iF = iF;
            this.sF = sF;
        }
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();
        discoSpi.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration singleCacheConfig = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheConfiguration(singleCacheConfig);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        cleanUpWorkDir();
    }

    /**
     *
     */
    private void cleanUpWorkDir() throws Exception {
        String workDir = U.defaultWorkDirectory();

        deleteRecursively(U.resolveWorkDirectory(workDir, "marshaller", false));
    }

    /**
     * Test checks a scenario when in multinode cluster one node may read marshaller mapping
     * from file storage and add it directly to marshaller context with accepted=true flag,
     * when another node sends a proposed request for the same mapping.
     *
     * In that case the request must not be marked as duplicate and must be processed in a regular way.
     * No hangs must take place.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-5401">IGNITE-5401</a> Take a look at JIRA ticket for more information about context of this test.
     *
     * This test must never hang on proposing of MarshallerMapping.
     */
    public void testFileMappingReadAndPropose() throws Exception {
        prepareMarshallerFileStore();

        IgniteEx ignite0 = startGrid(0);
        IgniteEx ignite1 = startGrid(1);

        BinaryObject obj0 = ignite0.binary().builder(SimpleValue.class.getName())
            .setField("iF", 10)
            .setField("sF", "str0")
            .build();

        BinaryObject obj1 = ignite0.binary().builder(SimpleValue.class.getName())
            .setField("iF", 20)
            .setField("sF", "str1")
            .build();

        IgniteCache<Object, Object> binCache = ignite0.cache(DEFAULT_CACHE_NAME).withKeepBinary();

        binCache.put(1, obj0);
        binCache.put(2, obj1);

        ignite0.cache(DEFAULT_CACHE_NAME).remove(1);

        ignite1.cache(DEFAULT_CACHE_NAME).put(3, new SimpleValue(30, "str2"));

        assertFalse(isDuplicateObserved);
    }

    /**
     *
     */
    private void prepareMarshallerFileStore() throws Exception {
        String typeName = SimpleValue.class.getName();
        int typeId = typeName.toLowerCase().hashCode();

        String fileName = typeId + ".classname0";

        File marshStoreDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), "marshaller", false);

        try(FileOutputStream out = new FileOutputStream(new File(marshStoreDir, fileName))) {
            try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writer.write(typeName);

                writer.flush();
            }
        }
    }

    /** */
    private class TestTcpDiscoverySpi extends TcpDiscoverySpi {

        /** */
        private class DiscoverySpiListenerWrapper implements DiscoverySpiListener {
            /** */
            private DiscoverySpiListener delegate;

            /**
             * @param delegate Delegate.
             */
            private DiscoverySpiListenerWrapper(DiscoverySpiListener delegate) {
                this.delegate = delegate;
            }

            /** {@inheritDoc} */
            @Override public void onDiscovery(
                int type,
                long topVer,
                ClusterNode node,
                Collection<ClusterNode> topSnapshot,
                @Nullable Map<Long, Collection<ClusterNode>> topHist,
                @Nullable DiscoverySpiCustomMessage spiCustomMsg
            ) {
                DiscoveryCustomMessage customMsg = spiCustomMsg == null ? null
                    : (DiscoveryCustomMessage) U.field(spiCustomMsg, "delegate");

                if (customMsg != null) {
                    //don't want to make this class public, using equality of class name instead of instanceof operator
                    if ("MappingProposedMessage".equals(customMsg.getClass().getSimpleName())) {
                        try {
                            isDuplicateObserved = U.invoke(MappingProposedMessage.class, customMsg, "duplicated");
                        }
                        catch (Exception e) {
                            log().error("Error when examining MappingProposedMessage.", e);
                        }
                    }
                }

                if (delegate != null)
                    delegate.onDiscovery(type, topVer, node, topSnapshot, topHist, spiCustomMsg);
            }

            /** {@inheritDoc} */
            @Override public void onLocalNodeInitialized(ClusterNode locNode) {
                // No-op.
            }
        }

        /** {@inheritDoc} */
        @Override public void setListener(@Nullable DiscoverySpiListener lsnr) {
            super.setListener(new DiscoverySpiListenerWrapper(lsnr));
        }
    }
}
