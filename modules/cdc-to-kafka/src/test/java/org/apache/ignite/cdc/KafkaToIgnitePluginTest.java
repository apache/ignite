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

package org.apache.ignite.cdc;

import java.io.File;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test for cache with custom groupId created.
 * Test for one cache of groupId.
 */
public class KafkaToIgnitePluginTest extends GridCommonAbstractTest {
    /** */
    private static File props;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!cfg.isClientMode()) {
            KafkaToIgnitePluginProvider provider = new KafkaToIgnitePluginProvider();

            provider.setPropertiesPath(props.getAbsolutePath());
            provider.setCaches("my-cache-2");

            cfg.setPluginProviders(provider);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        if (props == null) {
            props = File.createTempFile("kafka", "properties");

            props.deleteOnExit();
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        props = null;

        super.afterTestsStopped();
    }

    /** */
    @Test
    public void testPartitionSwitchOnNodeJoin() throws Exception {
        IgniteEx ign1 = startGrid(1);

        ign1.createCache("my-cache-1");
        ign1.createCache("my-cache-2");
        ign1.createCache("my-cache-3");

        awaitPartitionMapExchange();

        startGrid(2);

        awaitPartitionMapExchange();

        startClientGrid(4);

        startGrid(3);

        awaitPartitionMapExchange();

        ign1.destroyCache("my-cache-2");

        awaitPartitionMapExchange();

        ign1.createCache("my-cache-2");

        awaitPartitionMapExchange();

        stopGrid(2);

        awaitPartitionMapExchange();
    }
}
