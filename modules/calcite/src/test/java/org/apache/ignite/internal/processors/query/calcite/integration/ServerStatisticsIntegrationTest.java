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
 *
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.CalciteQueryProcessor;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.processors.query.stat.IgniteStatisticsManager;
import org.apache.ignite.internal.processors.query.stat.StatisticsKey;
import org.apache.ignite.internal.processors.query.stat.StatisticsTarget;
import org.apache.ignite.internal.processors.query.stat.config.StatisticsObjectConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for server side statistics usage.
 */
public class ServerStatisticsIntegrationTest extends AbstractBasicIntegrationTest {
    /** Server instance. */
    private IgniteEx srv;

    /**
     * Test cost with nulls for unknown values.
     */
    public static class TestCost {
        /** */
        Double rowCount;

        /** */
        Double cpu;

        /** */
        Double memory;

        /** */
        Double io;

        /** */
        Double network;

        /**
         * @return Row count.
         */
        public Double rowCount() {
            return rowCount;
        }

        /**
         * @return Cpu.
         */
        public Double cpu() {
            return cpu;
        }

        /**
         * @return Memory
         */
        public Double memory() {
            return memory;
        }

        /**
         * @return Io.
         */
        public Double io() {
            return io;
        }

        /**
         * @return Network.
         */
        public Double network() {
            return network;
        }

        /**
         * Constructor.
         *
         * @param rowCount Row count.
         * @param cpu Cpu.
         * @param memory Memory.
         * @param io Io.
         * @param network Network.
         */
        public TestCost(Double rowCount, Double cpu, Double memory, Double io, Double network) {
            this.rowCount = rowCount;
            this.cpu = cpu;
            this.memory = memory;
            this.io = io;
            this.network = network;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "TestCost{" +
                "rowCount=" + rowCount +
                ", cpu=" + cpu +
                ", memory=" + memory +
                ", io=" + io +
                ", network=" + network +
                '}';
        }
    }

    /**
     * Run select and check that cost take statisitcs in account:
     * 1) without statistics;
     * 2) with statistics;
     * 3) after deleting statistics.
     */
    @Test
    public void testQueryCostWithStatistics() throws IgniteCheckedException {
        createAndPopulateTable();
        StatisticsKey key = new StatisticsKey("PUBLIC", "PERSON");
        srv = ignite(0);

        TestCost costWoStats = new TestCost(1000., 1000., null, null, null);

        assertQuerySrv("select count(name) from person").matches(QueryChecker.containsCost(costWoStats)).check();

        clearQryCache(srv);

        collectStatistics(srv, key);

        TestCost costWithStats = new TestCost(3., 3., null, null, null);

        assertQuerySrv("select count(name) from person").matches(QueryChecker.containsCost(costWithStats)).check();

        statMgr(srv).dropStatistics(new StatisticsTarget(key));
        clearQryCache(srv);

        assertQuerySrv("select count(name) from person").matches(QueryChecker.containsCost(costWoStats)).check();
    }

    /**
     * Clear query cache in specified node.
     *
     * @param ign Ignite node to clear calcite query cache on.
     */
    protected void clearQryCache(IgniteEx ign) {
        CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
            (ign).context(), QueryEngine.class);

        qryProc.queryPlanCache().clear();
    }

    /**
     * Collect statistics by speicifed key on specified node.
     *
     * @param ign Node to collect statistics on.
     * @param key Statistics key to collect statistics by.
     * @throws IgniteCheckedException In case of errors.
     */
    protected void collectStatistics(IgniteEx ign, StatisticsKey key) throws IgniteCheckedException {
        IgniteStatisticsManager statMgr = statMgr(ign);

        statMgr.collectStatistics(new StatisticsObjectConfiguration(key));

        assertTrue(GridTestUtils.waitForCondition(() -> statMgr.getLocalStatistics(key) != null, 1000));
    }

    /**
     * Get statistics manager.
     *
     * @param ign Node to get statistics manager from.
     * @return IgniteStatisticsManager.
     */
    protected IgniteStatisticsManager statMgr(IgniteEx ign) {
        IgniteH2Indexing indexing = (IgniteH2Indexing)ign.context().query().getIndexing();

        return indexing.statsManager();
    }

    /** */
    protected QueryChecker assertQuerySrv(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return Commons.lookupComponent(srv.context(), QueryEngine.class);
            }
        };
    }
}
