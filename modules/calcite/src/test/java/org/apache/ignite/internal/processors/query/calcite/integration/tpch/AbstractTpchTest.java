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
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

/** */
@ParameterizedClass(name = "queryId={0}")
@MethodSource("parameters")
public abstract class AbstractTpchTest extends AbstractBasicIntegrationTest {
    /** */
    protected static final Collection<Integer> USED_TESTS = F.asList(15, 16, 17, 19, 20);

    /** Query ID. */
    @Parameter(0)
    public int qryId;

    /** */
    protected abstract double scale();

    /** */
    @MethodSource("parameters")
    public static Collection<Integer> parameters() {
        return USED_TESTS;
    }

    /** {@inheritDoc} */
    @BeforeAll
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
