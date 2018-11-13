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

package org.apache.ignite.internal.processors.query;

import java.util.List;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for correct distributed partitioned queries.
 */
public class IgniteSqlSplitterLeftJoinWithSubqueryHugeTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrid("srv0", "src/test/config/ignite-join-subquery-huge.xml");
        startGrid("srv1", "src/test/config/ignite-join-subquery-huge.xml");

        Ignition.setClientMode(true);
        try {
            startGrid("cli0", "src/test/config/ignite-join-subquery-huge.xml");
        }
        finally {
            Ignition.setClientMode(false);
        }
    }

    /**
     *
     */
    public void test() {
    }
    /**
     * @param plan Ignite query plan.
     */
    private void printPlan(List<List<?>> plan) {
        for (int i = 0; i < plan.size() - 1; ++i)
            System.out.println("MAP #" + i + ": " + plan.get(i).get(0));

        System.out.println("REDUCE: " + plan.get(plan.size() - 1).get(0));
    }

    /**
     * @param sql Query.
     * @return Result.
     */
    private List<List<?>> sql(String sql) {
        return grid("cli0").context().query().querySqlFields(
            new SqlFieldsQuery(sql), false).getAll();
    }
}
