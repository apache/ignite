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

import java.util.List;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.query.calcite.message.QueryStartRequest;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.junit.Test;

/** */
public class PartitionPruneTest extends AbstractBasicIntegrationTest {
    /** */
    private static final int ENTRIES_COUNT = 10000;

    /** */
    private static LongAdder qryStartCnt = new LongAdder();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getSqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration());

        cfg.setCommunicationSpi(new TcpCommunicationSpi() {
            /** {@inheritDoc} */
            @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC) {
                assert msg != null;

                if (GridIoMessage.class.isAssignableFrom(msg.getClass())) {
                    GridIoMessage gridMsg = (GridIoMessage)msg;

                    if (QueryStartRequest.class.isAssignableFrom(gridMsg.message().getClass()))
                        qryStartCnt.increment();
                }

                super.sendMessage(node, msg, ackC);
            }
        });

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sql("CREATE TABLE T1(ID INT, IDX_VAL VARCHAR, VAL VARCHAR, PRIMARY KEY(ID)) WITH cache_name=t1_cache,backups=1");
        sql("CREATE TABLE T2(ID INT, T1_ID INT, IDX_VAL VARCHAR, VAL VARCHAR, PRIMARY KEY(ID, T1_ID)) WITH " +
                "cache_name=t2_cache,backups=1,affinity_key=t1_id");
        sql("CREATE TABLE DICT(ID INT PRIMARY KEY, IDX_VAL VARCHAR, VAL VARCHAR) WITH template=replicated,cache_name=dict_cache");

        sql("CREATE INDEX T1_IDX ON T1(IDX_VAL)");
        sql("CREATE INDEX T2_IDX ON T2(IDX_VAL)");
        sql("CREATE INDEX T2_AFF ON T2(T1_ID)");
        sql("CREATE INDEX DICT_IDX ON DICT(IDX_VAL)");

        Stream.of("T1", "DICT").forEach(tableName -> {
            StringBuilder sb = new StringBuilder("INSERT INTO ").append(tableName)
                    .append("(ID, IDX_VAL, VAL) VALUES ");

            for (int i = 0; i < 10000; ++i) {
                sb.append("(").append(i).append(", ")
                        .append("'name_").append(i).append("', ")
                        .append("'name_").append(i).append("')");

                if (i < ENTRIES_COUNT - 1)
                    sb.append(",");
            }

            sql(sb.toString());

            assertEquals(ENTRIES_COUNT, client.getOrCreateCache(tableName + "_CACHE").size(CachePeekMode.PRIMARY));
        });

        qryStartCnt.reset();
    }

    /** */
    @Test
    public void testSimplePlanning() {
//        String sqlStr = "select * from T1 join T2 " +
//                " on T1.ID=T2.ID where T1.ID = 10 and T2.IDX_VAL <= 'test_1'";

//        String sqlStr = "select * from T1, T2 " +
//                " where T1._KEY = ? and T2.T1_ID = ? and T2.IDX_VAL <= ?";
        String sqlStr = "select * from T1 where T1.ID = 1 and T1.IDX_VAL = ?";

        List<?> res = sql(sqlStr, "name_1"); //, 10, "test_1");

        assertTrue(res.size() == 1);
    }
}
