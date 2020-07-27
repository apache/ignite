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
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage;
import org.apache.ignite.internal.processors.marshaller.MappingProposedMessage;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.spi.discovery.DiscoveryNotification;
import org.apache.ignite.spi.discovery.DiscoverySpiListener;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class IgniteMarshallerCacheFSRestoreTest extends GridCommonAbstractTest {
    /** */
    private volatile boolean isDuplicateObserved = true;

    /** */
    private boolean isPersistenceEnabled;

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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpDiscoverySpi discoSpi = new TestTcpDiscoverySpi();
        discoSpi.setIpFinder(LOCAL_IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration singleCacheCfg = new CacheConfiguration()
            .setName(DEFAULT_CACHE_NAME)
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAtomicityMode(CacheAtomicityMode.ATOMIC);

        cfg.setCacheConfiguration(singleCacheCfg);

        //persistence must be enabled to verify restoring mappings from FS case
        if (isPersistenceEnabled)
            cfg.setPersistentStoreConfiguration(new PersistentStoreConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
        cleanUpWorkDir();
        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        cleanPersistenceDir();
    }

    /**
     *
     */
    private void cleanUpWorkDir() throws Exception {
        String workDir = U.defaultWorkDirectory();

        U.delete(U.resolveWorkDirectory(workDir, DataStorageConfiguration.DFLT_MARSHALLER_PATH, false));
    }

    /**
     * Test checks a scenario when in multinode cluster one node may read marshaller mapping
     * from file storage and add it directly to marshaller context with accepted=true flag,
     * when another node sends a proposed request for the same mapping.
     *
     * In that case the request must not be marked as duplicate and must be processed in a regular way.
     * No hangs must take place.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-5401">IGNITE-5401</a> JIRA ticket
     * provides more information about context of this test.
     *
     * This test must never hang on proposing of MarshallerMapping.
     */
    @Test
    public void testFileMappingReadAndPropose() throws Exception {
        isPersistenceEnabled = false;

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

        File marshStoreDir = U.resolveWorkDirectory(U.defaultWorkDirectory(), DataStorageConfiguration.DFLT_MARSHALLER_PATH, false);

        try (FileOutputStream out = new FileOutputStream(new File(marshStoreDir, fileName))) {
            try (Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
                writer.write(typeName);

                writer.flush();
            }
        }
    }

    /**
     * Verifies scenario that node with corrupted marshaller mapping store must fail on startup
     * with appropriate error message.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IGNITE-6536">IGNITE-6536</a> JIRA provides more information
     * about this case.
     */
    @Test
    public void testNodeStartFailsOnCorruptedStorage() throws Exception {
        isPersistenceEnabled = true;

        Ignite ig = startGrids(3);

        ig.active(true);

        ig.cache(DEFAULT_CACHE_NAME).put(0, new SimpleValue(0, "value0"));

        stopAllGrids();

        corruptMarshallerStorage();

        try {
            startGrid(0);
        }
        catch (IgniteCheckedException e) {
            verifyException((IgniteCheckedException) e.getCause());
        }
    }

    /**
     * Class name for CustomClass class mapping file gets cleaned up from file system.
     */
    private void corruptMarshallerStorage() throws Exception {
        Path marshallerDir = Paths.get(U.defaultWorkDirectory(), DataStorageConfiguration.DFLT_MARSHALLER_PATH);

        File[] storedMappingsFiles = marshallerDir.toFile().listFiles();

        assert storedMappingsFiles.length == 1;

        try (FileOutputStream out = new FileOutputStream(storedMappingsFiles[0])) {
            out.getChannel().truncate(0);
        }
    }

    /** */
    private void verifyException(IgniteCheckedException e) throws Exception {
        String msg = e.getMessage();

        if (msg == null || !msg.contains("Class name is null"))
            throw new Exception("Exception with unexpected message was thrown: " + msg, e);
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
            @Override public IgniteFuture<?> onDiscovery(
                DiscoveryNotification notification
            ) {
                DiscoveryCustomMessage customMsg = notification.getCustomMsgData() == null ? null
                    : (DiscoveryCustomMessage) U.field(notification.getCustomMsgData(), "delegate");

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
                    return delegate.onDiscovery(notification);

                return new IgniteFinishedFutureImpl<>();
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
