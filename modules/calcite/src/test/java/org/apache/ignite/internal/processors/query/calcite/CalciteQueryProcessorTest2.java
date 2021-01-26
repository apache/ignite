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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_EXPERIMENTAL_SQL_ENGINE;

/** */
@WithSystemProperty(key = "calcite.debug", value = "false")
@WithSystemProperty(key = IGNITE_EXPERIMENTAL_SQL_ENGINE, value = "true")
public class CalciteQueryProcessorTest2 extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() {
        for (Ignite ign : G.allGrids()) {
            for (String cacheName : ign.cacheNames())
                ign.destroyCache(cacheName);

            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }
    }

    /**
     * Test verifies that AssertionError on fragment deserialization phase doesn't lead to execution freezes.
     *
     * 1) Start several nodes.
     * 2) Replace CommunicationSpi to one that modifies messages (replace join type inside a QueryStartRequest).
     * 3) Execute query that requires CNLJ.
     * 4) Verify that query failed with proper exception.
     */
    @Test
    public void query() throws Exception {
        startGrid(createConfiguration(1, false));
        startGrid(createConfiguration(2, false));

        IgniteEx client = startGrid(createConfiguration(0, true));

        sql(client, "create table test (id int primary key, val varchar)");

        String sql = "select /*+ DISABLE_RULE('NestedLoopJoinConverter', 'MergeJoinConverter') */ t1.id from test t1, test t2 where t1.id = t2.id";

        Throwable t = GridTestUtils.assertThrowsWithCause(() -> sql(client, sql), AssertionError.class);
        assertEquals("only INNER join supported by IgniteCorrelatedNestedLoop", t.getCause().getMessage());
    }

    /** */
    private List<List<?>> sql(IgniteEx ignite, String sql) {
        return ignite.context().query().querySqlFields(new SqlFieldsQuery(sql).setSchema("PUBLIC"), false).getAll();
    }

    /** */
    private IgniteConfiguration createConfiguration(int id, boolean client) throws Exception {
        return getConfiguration(client ? "client" : "server-" + id)
            .setClientMode(client)
            .setCommunicationSpi(new TcpCommunicationSpi() {
                /** {@inheritDoc} */
                @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                    if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof QueryStartRequest) {
                        QueryStartRequest req = (QueryStartRequest)((GridIoMessage)msg).message();

                        String root = GridTestUtils.getFieldValue(req, "root");

                        GridTestUtils.setFieldValue(req, "root",
                            root.replace("\"joinType\":\"inner\"", "\"joinType\":\"full\""));
                    }

                    super.sendMessage(node, msg, ackC);
                }
            });
    }
}
