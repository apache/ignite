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

package org.apache.ignite.plugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for Ignite plugin configuration.
 */
public class PluginConfigurationTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * Tests that {@link ServiceLoader#load(Class)} result will be used if plugin providers is not configured
     * explicitly.
     */
    @Test
    public void testNullPluginProviders() throws Exception {
        doTest(null, U.allPluginProviders());
    }

    /**
     * Tests that {@link ServiceLoader#load(Class)} result will be used if plugin providers is empty for
     * {@link IgniteConfiguration}.
     */
    @Test
    @SuppressWarnings("ZeroLengthArrayAllocation")
    public void testEmptyPluginProviders() throws Exception {
        doTest(new PluginProvider[]{}, U.allPluginProviders());
    }

    /**
     * Tests that explicitly configured plugin providers will be used.
     */
    @Test
    public void testNotEmptyPluginProviders() throws Exception {
        TestPluginProvider testPluginProvider = new TestPluginProvider();

        doTest(new PluginProvider[]{testPluginProvider}, Collections.singletonList(testPluginProvider));
    }

    /**
     * Asserts expectations.
     *
     * @param cfgProviders Config providers.
     * @param expProviders Expected providers.
     */
    @SuppressWarnings("rawtypes")
    private void doTest(PluginProvider<?>[] cfgProviders, List<PluginProvider> expProviders) throws Exception {
        List<String> exp = toClasses(expProviders);

        IgniteConfiguration cfg = getConfiguration();

        cfg.setPluginProviders(cfgProviders);

        IgniteEx ignite = startGrid(cfg);

        List<String> providers = toClasses(ignite.context().plugins().allProviders());

        assertEqualsCollections(exp, providers);
    }

    /**
     * @param col Collection of plugin providers
     */
    @SuppressWarnings("rawtypes")
    private static List<String> toClasses(Collection<PluginProvider> col) {
        return col.stream().map(PluginProvider::name).collect(Collectors.toList());
    }

    /** */
    public static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "test_plugin";
        }
    }
}
