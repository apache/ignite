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

import com.google.common.base.Preconditions;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.WALMode;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class IgniteTcBotSandboxTest extends GridCommonAbstractTest {

    public static final String TEST_HIST_CACHE_NAME = "teamcityTestRunHist";

    @Test
    public void readTcBotDb() throws Exception {
        //IgniteConfiguration cfg = new IgniteConfiguration();
       // Ignite start = Ignition.start(cfg);
        IgniteEx ignite = startGrid(0);
        ignite.cluster().active(true);

        IgniteCache<Object, Object> cache = ignite.cache(TEST_HIST_CACHE_NAME);
        Preconditions.checkNotNull(cache, "Cache should be present, present caches ["  + ignite.cacheNames() + "]");
        IgniteCache<BinaryObject, BinaryObject> entries = cache.withKeepBinary();
        IgniteBinary binary = ignite.binary();
        BinaryObjectBuilder builder = binary.builder("org.apache.ignite.ci.teamcity.ignited.runhist.RunHistKey");

        builder.setField("srvId", 1411517106);
        builder.setField("testOrSuiteName", 11924);
        builder.setField("branch", 281);

        //idHash=1028871081, hash=1241170874, srvId=1411517106, testOrSuiteName=11924, branch=281

        BinaryObject key = builder.build();
        BinaryObject val = entries.get(key);

        System.out.println(val);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setWorkDirectory("C:\\projects\\corrupt\\work"); // todo set correct path

        cfg.setConsistentId("TcHelper");

        DataRegionConfiguration regCfg = new DataRegionConfiguration()
            .setMaxSize(2L * 1024 * 1024 * 1027)
            .setPersistenceEnabled(true);

        DataStorageConfiguration dsCfg = new DataStorageConfiguration()
            .setWalMode(WALMode.LOG_ONLY)
            .setDefaultDataRegionConfiguration(regCfg);

        cfg.setDataStorageConfiguration(dsCfg);

        return cfg;
    }
}
