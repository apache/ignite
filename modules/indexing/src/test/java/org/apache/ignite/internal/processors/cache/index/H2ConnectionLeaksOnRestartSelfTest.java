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

package org.apache.ignite.internal.processors.cache.index;

import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.GridDebug;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for leaks JdbcConnection on SqlFieldsQuery execute.
 */
public class H2ConnectionLeaksOnRestartSelfTest extends GridCommonAbstractTest {
    /** Cache name. */
    private static final String CACHE_NAME = "cache";

    /** Nodes count. */
    private static final int NODE_CNT = 1;

    /** Keys count. */
    private static final int KEY_CNT = 100_000;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration<Long, String> ccfg = new CacheConfiguration<Long, String>().setName(CACHE_NAME)
            .setIndexedTypes(Long.class, String.class);

        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }

    /**
     * @throws Exception On failed.
     */
    public void test() throws Exception {
        int cnt = 0;

        while (true) {
            Ignite node = startGrids(NODE_CNT);

            try (IgniteDataStreamer<Long, String> stream = node.dataStreamer(CACHE_NAME)) {
                stream.allowOverwrite(false);

                for (int i = 0; i < KEY_CNT; i++)
                    stream.addData((long)i, UUID.randomUUID().toString());
            }

            U.sleep(50);

            stopAllGrids();

            GridDebug.dumpHeap(String.format("heap%d.hprof", cnt++ % 10), true);
        }
    }

}
