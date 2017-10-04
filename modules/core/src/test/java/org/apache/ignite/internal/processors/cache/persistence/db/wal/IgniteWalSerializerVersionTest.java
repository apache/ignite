/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.RecordSerializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV1Serializer;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordV2Serializer;
import org.apache.ignite.internal.util.typedef.internal.GPC;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_WAL_SERIALIZER_VERSION;

/**
 *
 */
public class IgniteWalSerializerVersionTest extends GridCommonAbstractTest {
    /** Ip finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(IP_FINDER);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCheckDifferentSerializerVersions() throws Exception {
        IgniteEx ig0 = (IgniteEx)startGrid();

        IgniteWriteAheadLogManager wal0 = ig0.context().cache().context().wal();

        RecordSerializer ser0 = U.field(wal0, "serializer");

        assertTrue(ser0 instanceof RecordV1Serializer);

        stopGrid();

        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "2");

        IgniteEx ig1 = (IgniteEx)startGrid();

        IgniteWriteAheadLogManager wal1 = ig1.context().cache().context().wal();

        RecordSerializer ser1 = U.field(wal1, "serializer");

        assertTrue(ser1 instanceof RecordV2Serializer);

        stopGrid();

        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "3");

        GridTestUtils.assertThrowsAnyCause(log, new GPC<Void>() {
            @Override public Void call() throws Exception {
                startGrid();

                return null;
            }
        }, IgniteCheckedException.class, "Failed to create a serializer with the given version");

        System.setProperty(IGNITE_WAL_SERIALIZER_VERSION, "1");

        IgniteEx ig2 = (IgniteEx)startGrid();

        IgniteWriteAheadLogManager wal2 = ig2.context().cache().context().wal();

        RecordSerializer ser2 = U.field(wal2, "serializer");

        assertTrue(ser2 instanceof RecordV1Serializer);

        stopGrid();
    }
}
