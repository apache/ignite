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

package org.apache.ignite.internal;


import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Check logging local node metrics
 */
@SuppressWarnings({"ProhibitedExceptionDeclared"})
@GridCommonTest(group = "Kernal")
public class GridNodeMetricsLogSelfTest extends GridCommonAbstractTest {
    /** */

    public GridNodeMetricsLogSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeMetricsLog() throws Exception {
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setMetricsLogFrequency(1000);

        startGrid(1);
        Ignite g1 = startGrid("1", cfg);
        IgniteCache<Integer, String> cache1 = g1.createCache("TestCache1");
        cache1.put(1, "one");

        Ignite g2 = startGrid("2", cfg);
        IgniteCache<Integer, String> cache2 = g2.createCache("TestCache2");
        cache2.put(2, "two");

        Thread.sleep(10000);
        String fName  = g1.log().fileName();

        System.out.println(cache1.get(1));
        System.out.println(cache2.get(2));

        //Check that nodes are alie
        assert cache1.get(1).equals("one");
        assert cache2.get(2).equals("two");

        stopAllGrids();

        File f = new File(fName);
        String fullLog = new String(GridTestUtils.readFile(f), StandardCharsets.UTF_8);

        assert fullLog.contains("Metrics for local node");
        assert fullLog.contains("uptime=");
        assert fullLog.contains("Non heap");
        assert fullLog.contains("Outbound messages queue");
    }
}
