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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.ClusterNodeAttributeAffinityBackupFilter;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.jupiter.api.Test;

/** */
public class MultiDcQueryMappingTest extends AbstractBasicIntegrationTest {
    /** */
    private static final String DC1 = "DC1";

    /** */
    private static final String DC2 = "DC2";

    /** */
    private static final int ROWS_CNT = 100;

    /** */
    private String dcId;

    /** */
    AtomicBoolean crossDcRequest = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                if (msg instanceof GridIoMessage) {
                    GridIoMessage msg0 = (GridIoMessage)msg;

                    if (msg0.message() instanceof QueryStartRequest) {
                        if (!ignite().cluster().localNode().dataCenterId().equals(node.dataCenterId()))
                            crossDcRequest.set(true);
                    }
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        cfg.setUserAttributes(F.asMap(IgniteSystemProperties.IGNITE_DATA_CENTER_ID, dcId));

        // Partitioned cache, in both data centers.
        CacheConfiguration<Integer, Employer> ccfgPart2dc = cacheConfiguration("part2dc")
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setAffinity(new RendezvousAffinityFunction().setAffinityBackupFilter(
                new ClusterNodeAttributeAffinityBackupFilter(IgniteSystemProperties.IGNITE_DATA_CENTER_ID)));

        // Replicated cache, in both data centers.
        CacheConfiguration<Integer, Employer> ccfgRepl2dc = cacheConfiguration("repl2dc")
            .setCacheMode(CacheMode.REPLICATED);

        // Partitioned cache, in one data center.
        CacheConfiguration<Integer, Employer> ccfgPart1dc = cacheConfiguration("part1dc")
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setNodeFilter(n -> n.dataCenterId().equals(DC1));

        // Replicated cache, in one data center.
        CacheConfiguration<Integer, Employer> ccfgRepl1dc = cacheConfiguration("repl1dc")
            .setCacheMode(CacheMode.REPLICATED)
            .setNodeFilter(n -> n.dataCenterId().equals(DC2));

        cfg.setCacheConfiguration(ccfgPart2dc, ccfgRepl2dc, ccfgPart1dc, ccfgRepl1dc);

        return cfg;
    }

    /** */
    private CacheConfiguration<Integer, Employer> cacheConfiguration(String name) {
        return new CacheConfiguration<Integer, Employer>()
            .setName(name)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setQueryEntities(Collections.singletonList(new QueryEntity(Integer.class, Employer.class)
                .setTableName(name)
                .addQueryField("ID", Integer.class.getName(), "ID")
                .setKeyFieldName("ID")
            ));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op.
    }

    /** */
    @Test
    public void testQueryMapping() throws Exception {
        dcId = DC1;

        IgniteEx srv1dc1 = startGrid(0);
        IgniteEx srv2dc1 = startGrid(1);
        IgniteEx clientDc1 = startClientGrid(2);

        dcId = DC2;

        IgniteEx srv1dc2 = startGrid(3);
        IgniteEx srv2dc2 = startGrid(4);
        IgniteEx clientDc2 = startClientGrid(5);

        fillTable(clientDc1, "part2dc");
        fillTable(clientDc1, "repl2dc");
        fillTable(clientDc1, "part1dc");
        fillTable(clientDc1, "repl1dc");

        checkQueries(clientDc1);
        checkQueries(clientDc2);
        checkQueries(srv1dc1);
        checkQueries(srv1dc2);
        checkQueries(srv2dc1);
        checkQueries(srv2dc2);
    }

    /** */
    private void fillTable(IgniteEx ignite, String name) {
        for (int i = 0; i < ROWS_CNT; i++)
            sql(ignite, "INSERT INTO " + name + "(ID, NAME, SALARY) VALUES (?, ?, ?)", i, "name" + i, i);
    }

    /** */
    private void checkQueries(IgniteEx ignite) {
        boolean dc1 = DC1.equals(ignite.context().discovery().localNode().dataCenterId());
        checkQuery(ignite, "SELECT * FROM part2dc", false);
        checkQuery(ignite, "SELECT * FROM repl2dc", false);
        checkQuery(ignite, "SELECT * FROM part1dc", !dc1);
        checkQuery(ignite, "SELECT * FROM repl1dc", dc1);
        checkQuery(ignite, "SELECT * FROM part2dc JOIN repl2dc USING (name)", false);
        checkQuery(ignite, "SELECT * FROM part1dc JOIN repl1dc USING (name)", true);
    }

    /** */
    private void checkQuery(IgniteEx ignite, String sql, boolean expCrossDc) {
        crossDcRequest.set(false);

        List<?> res = sql(ignite, sql);

        assertEquals(ROWS_CNT, res.size());

        assertEquals(expCrossDc, crossDcRequest.get());
    }
}
