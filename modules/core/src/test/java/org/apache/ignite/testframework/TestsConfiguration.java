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

package org.apache.ignite.testframework;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.testframework.config.ConfigurationFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Immutable tests configuration.
 */
public class TestsConfiguration {
    /** */
    private final ConfigurationFactory factory;

    /** */
    private final String suffix;

    /** */
    private final boolean stopNodes;

    /** */
    private final int gridCnt;

    /** */
    private final CacheStartMode cacheStartMode;

    /** */
    private final MultiNodeTestsConfiguration multiNodeCfg;

    /** */
    private boolean startCache;

    /** */
    private boolean stopCache;

    /**
     * @param factory Factory.
     * @param suffix Class suffix.
     * @param stopNodes Stope nodes.
     * @param gridCnt Grdi count.
     */
    public TestsConfiguration(ConfigurationFactory factory,
        String suffix,
        boolean stopNodes,
        CacheStartMode cacheStartMode,
        int gridCnt) {
        this(factory, suffix, stopNodes, true, true, cacheStartMode, gridCnt, null);
    }

    /**
     * @param factory Factory.
     * @param suffix Class suffix.
     * @param stopNodes Stope nodes.
     * @param gridCnt Grdi count.
     */
    public TestsConfiguration(ConfigurationFactory factory,
        String suffix,
        boolean stopNodes,
        boolean startCache,
        boolean stopCache,
        CacheStartMode cacheStartMode,
        int gridCnt,
        @Nullable MultiNodeTestsConfiguration multiNodeCfg) {
        A.ensure(gridCnt >= 1, "Grids count cannot be less then 1.");

        this.factory = factory;
        this.suffix = suffix;
        this.stopNodes = stopNodes;
        this.gridCnt = gridCnt;
        this.cacheStartMode = cacheStartMode;
        this.multiNodeCfg = multiNodeCfg;
        this.startCache = startCache;
        this.stopCache = stopCache;
    }

    /**
     * @return Configuration factory.
     */
    public ConfigurationFactory configurationFactory() {
        return factory;
    }

    /**
     * @return Test class name suffix.
     */
    public String suffix() {
        return suffix;
    }

    /**
     * @return Grids count.
     */
    public int gridCount() {
        return gridCnt;
    }

    /**
     * @return Whether nodes should be stopped after tests execution or not.
     */
    public boolean isStopNodes() {
        return stopNodes;
    }

    /**
     * @return Cache start type.
     */
    public CacheStartMode cacheStartMode() {
        return cacheStartMode;
    }

    /**
     * @return Multi node config.
     */
    public MultiNodeTestsConfiguration multiNodeConfig() {
        return multiNodeCfg;
    }

    /**
     * @return Whether cache should be started before tests execution or not.
     */
    public boolean isStartCache() {
        return startCache;
    }

    /**
     * @return Whether cache should be destroyed after tests execution or not.
     */
    public boolean isStopCache() {
        return stopCache;
    }

    /**
     *
     */
    public static class MultiNodeTestsConfiguration {
        /** */
        private final int testedNodeIdx;

        /** Custome number of tests. */
        private final int numOfTests;

        /**
         * @param testedNodeIdx Tested node type.
         * @param numOfTests
         */
        public MultiNodeTestsConfiguration(int testedNodeIdx, int numOfTests) {
            this.testedNodeIdx = testedNodeIdx;
            this.numOfTests = numOfTests;
        }

        /**
         * @return Number of tests.
         */
        public int numOfTests() {
            return numOfTests;
        }

        /**
         * @return Index of tested node.
         */
        public int testedNodeIndex() {
            return testedNodeIdx;
        }
    }
}
