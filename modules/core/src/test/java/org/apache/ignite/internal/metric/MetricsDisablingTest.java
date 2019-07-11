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

package org.apache.ignite.internal.metric;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.NoOpMetricRegistry;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.cacheMetricsRegistryName;

/** */
public class MetricsDisablingTest extends GridCommonAbstractTest {
    /** */
    public static final String DEFAULT_CACHE_REGISTRY = cacheMetricsRegistryName(DEFAULT_CACHE_NAME, false);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        cfg.setDisabledMetricRegistries(DEFAULT_CACHE_REGISTRY);

        return cfg;
    }

    /** */
    @Test
    public void testMetricDisabling() throws Exception {
        IgniteEx g = startGrid("test");

        MetricRegistry mreg = g.context().metric().registry(DEFAULT_CACHE_REGISTRY);

        assertTrue(mreg instanceof NoOpMetricRegistry);

        g.createCache("some-cache");

        mreg = g.context().metric().registry(cacheMetricsRegistryName("some-cache", false));

        assertTrue(mreg instanceof MetricRegistryImpl);
    }

}
