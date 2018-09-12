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

package org.apache.ignite.internal.processors.cache.index;

import java.util.concurrent.Callable;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class SqlTransactionsComandsWithMvccDisabledSelfTest extends AbstractSchemaSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(commonConfiguration(0));

        super.execute(grid(0), "CREATE TABLE INTS(k int primary key, v int) WITH \"wrap_value=false,cache_name=ints," +
            "atomicity=transactional\"");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }


    /**
     * @throws Exception if failed.
     */
    public void testBeginWithMvccDisabledThrows() throws Exception {
        checkMvccDisabledBehavior("BEGIN");
    }

    /**
     * @throws Exception if failed.
     */
    public void testCommitWithMvccDisabledThrows() throws Exception {
        checkMvccDisabledBehavior("COMMIT");
    }

    /**
     * @throws Exception if failed.
     */
    public void testRollbackWithMvccDisabledThrows() throws Exception {
        checkMvccDisabledBehavior("rollback");
    }

    /**
     * @param sql Operation to test.
     * @throws Exception if failed.
     */
    private void checkMvccDisabledBehavior(String sql) throws Exception {
        try (IgniteEx node = startGrid(commonConfiguration(1))) {
            GridTestUtils.assertThrows(null, new Callable<Object>() {
                @Override public Object call() throws Exception {
                    execute(node, sql);

                    return null;
                }
            }, IgniteSQLException.class, "MVCC must be enabled in order to invoke transactional operation: " + sql);
        }
    }
}
