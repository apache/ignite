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

package org.apache.ignite.cache.affinity.rendezvous;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class DcAffinityBackupFilterSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String DC_0_ID = "DC_0";

    /** */
    private static final String DC_1_ID = "DC_1";

    @Override protected IgniteConfiguration optimize(IgniteConfiguration cfg) throws IgniteCheckedException {
        return super.optimize(cfg).setIncludeProperties((String[])null);
    }

    @Test
    public void test1() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_0_ID);
        startGrids(3);

        System.setProperty(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, DC_1_ID);
        startGrid(3);
        startGrid(4);
        IgniteEx srv = startGrid(5);

        CacheConfiguration ccfg = defaultCacheConfiguration();
        ccfg.setBackups(3);
        ccfg.setAffinity(new RendezvousAffinityFunction(false, 2)
            .setAffinityBackupFilter(new DcAffinityBackupFilter(2, 3)));

        srv.getOrCreateCache(ccfg);
    }
}
