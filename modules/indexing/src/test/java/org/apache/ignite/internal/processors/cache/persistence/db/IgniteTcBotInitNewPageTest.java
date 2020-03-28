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

package org.apache.ignite.internal.processors.cache.persistence.db;

import com.google.common.base.Strings;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.record.delta.InitNewPageRecord;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test creates a lot of index pages in the cache with low number of partitions.<br>
 * Then cache entries are removed to enforce all pages to come to a free list. <br>
 * Then creation of data pages with long data will probably result in page rotation.<br>
 * Expected behaviour: all {@link InitNewPageRecord} should have consistent partition IDs.
 */
public class IgniteTcBotInitNewPageTest extends GridCommonAbstractTest {
    /** Cache name. */
    public static final String CACHE = "cache";

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** */
    @Test
    public void testInitNewPagePageIdConsistency() throws Exception {
        IgniteEx ignite = startGrid(0);

        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.cache(CACHE);

        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(CACHE)) {
            for (int i = 0; i < SF.apply(1_000_000); i++)
                ds.addData(i, i);
        }

        cache.clear();

        String longStr = Strings.repeat("Apache Ignite", SF.apply(1000));

        try (IgniteDataStreamer<Object, Object> ds = ignite.dataStreamer(CACHE)) {
            for (int i = 0; i < 1_000; i++)
                ds.addData(i, longStr);
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<Integer, Object> ccfg = new CacheConfiguration<>(CACHE);

        ccfg.setAffinity(new RendezvousAffinityFunction(false, 4));

        cfg.setCacheConfiguration(ccfg);

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
            .setMaxSize(SF.apply(128) * 1024 * 1024)
            .setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(regCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }
}
