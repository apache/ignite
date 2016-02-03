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

/**
 *
 */
public class NewTestsConfiguration {
    /** */
    private final ConfigurationFactory factory;

    /** */
    private final String suffix;

    /** */
    private final boolean stopNodes;

    /** */
    private final int gridCnt;

    /**
     * @param factory Factory.
     * @param suffix Class suffix.
     * @param stopNodes Stope nodes.
     * @param gridCnt Grdi count.
     */
    public NewTestsConfiguration(ConfigurationFactory factory, String suffix, boolean stopNodes, int gridCnt) {
        A.ensure(gridCnt >= 1, "Grids count cannot be less then 1.");

        this.factory = factory;
        this.suffix = suffix;
        this.stopNodes = stopNodes;
        this.gridCnt = gridCnt;
    }

    /**
     * @param factory Factory.
     * @param suffix Class suffix.
     * @param gridCnt Grdi count.
     */
    public NewTestsConfiguration(ConfigurationFactory factory, String suffix, int gridCnt) {
        this(factory, suffix, false, gridCnt);
    }

    /**
     * @param factory Factory.
     * @param suffix Class suffix.
     */
    public NewTestsConfiguration(ConfigurationFactory factory, String suffix) {
        this(factory, suffix, false, 1);
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
}
