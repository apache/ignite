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

package org.apache.ignite.internal.processors.query.calcite.integration.tpch;

import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.junit.Test;

/** */
public class TpchTest extends AbstractBasicIntegrationTest {
    /**
     * Exception in TPC-H Query #20:
     *   Unexpected error at query optimizer: Index 7 out of bounds for length 7
     */
    @Test
    public void testQ20() throws InterruptedException {
        TpchHelper.createTables(client);

        // Reproduced without data in fact
//        TpchHelper.fillTables(client, 0.01);
//        awaitPartitionMapExchange();

        sql(TpchHelper.getQuery(20));
    }

    @Test
    public void testQ18() throws InterruptedException {
        TpchHelper.createTables(client);

        TpchHelper.fillTables(client, 1.0);
        awaitPartitionMapExchange();

        sql(TpchHelper.getQuery(18));
    }

    @Test
    public void testQ8() throws InterruptedException {
        TpchHelper.createTables(client);

        TpchHelper.fillTables(client, 1.0);
        awaitPartitionMapExchange();

        sql(TpchHelper.getQuery(8));
    }

}
