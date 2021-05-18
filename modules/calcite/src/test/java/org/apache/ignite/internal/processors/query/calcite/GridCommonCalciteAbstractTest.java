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

package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.RunningFragmentInfo;
import org.apache.ignite.internal.processors.query.RunningQueryInfo;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Assert;

import static org.apache.ignite.internal.processors.query.calcite.CalciteTestUtils.queryProcessor;

/**
 *
 */
public class GridCommonCalciteAbstractTest extends GridCommonAbstractTest {

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        checkQueryDeregistered();
    }

    /**
     *
     */
    protected void cleanQueryPlanCache() {
        for (Ignite ign : G.allGrids()) {
            CalciteQueryProcessor qryProc = (CalciteQueryProcessor)Commons.lookupComponent(
                ((IgniteEx)ign).context(), QueryEngine.class);

            qryProc.queryPlanCache().clear();
        }
    }

    private void checkQueryDeregistered() {
        for (Ignite ign : G.allGrids()) {
            CalciteQueryProcessor processor = queryProcessor((IgniteEx)ign);

            List<RunningQueryInfo> queries = processor.runningQueries();
            List<RunningFragmentInfo> fragments = processor.runningFragments();

            //Due to deregistering process is asynchronous need to give a chance finish it.
            if (!(queries.isEmpty() && fragments.isEmpty())) {
                doSleep(300);

                queries = processor.runningQueries();
                fragments = processor.runningFragments();
            }

            UUID nodeId = ((IgniteEx)ign).context().localNodeId();
            queries.forEach(q -> System.out.println("STILL RUNNING QUERY " + nodeId + "- " + q.query()));
            queries.forEach(f -> System.out.println("STILL RUNNING FRAGMENTS " + nodeId + " id-" + f.qryId() + "- " + f.query()));

            Assert.assertTrue("Grid still have running queries", queries.isEmpty());
            Assert.assertTrue("Grid still have running fragments", fragments.isEmpty());
        }
    }
}
