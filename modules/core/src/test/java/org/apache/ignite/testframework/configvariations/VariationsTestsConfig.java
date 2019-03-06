/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.testframework.configvariations;

import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Immutable tests configuration.
 */
public class VariationsTestsConfig {
    /** */
    private final ConfigFactory factory;

    /** */
    private final String desc;

    /** */
    private final boolean stopNodes;

    /** */
    private final int gridCnt;

    /** */
    private final CacheStartMode cacheStartMode;

    /** */
    private final int testedNodeIdx;

    /** */
    private boolean startCache;

    /** */
    private boolean stopCache;

    /** */
    private boolean awaitPartMapExchange;

    /** */
    private boolean withClients;

    /**
     * @param factory Factory.
     * @param desc Class suffix.
     * @param stopNodes Stope nodes.
     * @param gridCnt Grdi count.
     */
    public VariationsTestsConfig(
        ConfigFactory factory,
        String desc,
        boolean stopNodes,
        CacheStartMode cacheStartMode,
        int gridCnt,
        boolean awaitPartMapExchange
    ) {
        this(factory, desc, stopNodes, true, true, cacheStartMode, gridCnt, 0, false, awaitPartMapExchange);
    }

    /**
     * @param factory Factory.
     * @param desc Config description.
     * @param stopNodes Stope nodes.
     * @param gridCnt Grid count.
     */
    public VariationsTestsConfig(
        ConfigFactory factory,
        String desc,
        boolean stopNodes,
        boolean startCache,
        boolean stopCache,
        CacheStartMode cacheStartMode,
        int gridCnt,
        int testedNodeIdx,
        boolean withClients,
        boolean awaitPartMapExchange
    ) {
        A.ensure(gridCnt >= 1, "Grids count cannot be less than 1.");

        this.factory = factory;
        this.desc = desc;
        this.gridCnt = gridCnt;
        this.cacheStartMode = cacheStartMode;
        this.testedNodeIdx = testedNodeIdx;
        this.stopNodes = stopNodes;
        this.startCache = startCache;
        this.stopCache = stopCache;
        this.withClients = withClients;
        this.awaitPartMapExchange = awaitPartMapExchange;
    }

    /**
     * @return Configuration factory.
     */
    public ConfigFactory configurationFactory() {
        return factory;
    }

    /**
     * @return Configuration description..
     */
    public String description() {
        return desc;
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
     * @return Index of node which should be tested or {@code null}.
     */
    public int testedNodeIndex() {
        return testedNodeIdx;
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
     * @return With clients.
     */
    public boolean withClients() {
        return withClients;
    }

    /**
     * @return Partition map exchange wait flag.
     */
    public boolean awaitPartitionMapExchange() {
        return awaitPartMapExchange;
    }
}
