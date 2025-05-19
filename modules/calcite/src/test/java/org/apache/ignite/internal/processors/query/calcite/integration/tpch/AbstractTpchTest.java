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

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public abstract class AbstractTpchTest extends AbstractBasicIntegrationTest {
    /**
     * All the knows queries.
     * TODO: IGNITE-25209 for q15 and q17
     */
    protected static final Collection<Integer> QUERIES = IntStream.range(21, 22)
        .filter(q -> q != 15)
        .filter(q -> q != 17)
        .boxed().collect(Collectors.toSet());

    /** */
    @Parameterized.Parameters(name = "queryId={0}")
    public static Collection<Integer> params() {
        return QUERIES;
    }

    /** Query ID. */
    @Parameterized.Parameter
    public int qryId;

    /** */
    protected abstract double scale();

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        TpchHelper.createTables(client);

        TpchHelper.fillTables(client, scale());

        TpchHelper.collectSqlStatistics(client);
    }

    /** {@inheritDoc} */
    @Override protected boolean destroyCachesAfterTest() {
        return false;
    }

    /**
     * Test the TPC-H query can be planned and executed.
     */
    @Test
    public void test() {
        sql(TpchHelper.getQuery(qryId));
    }
}
