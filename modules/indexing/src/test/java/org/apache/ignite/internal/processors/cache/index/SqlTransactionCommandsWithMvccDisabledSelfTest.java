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
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class SqlTransactionCommandsWithMvccDisabledSelfTest extends AbstractSchemaSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(commonConfiguration(0));

        super.execute(grid(0), "CREATE TABLE INTS(k int primary key, v int) WITH \"wrap_value=false,cache_name=ints," +
            "atomicity=transactional\"");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testBeginWithMvccDisabled() throws Exception {
        GridTestUtils.assertThrows(null, new Callable<Object>() {
            @Override public Object call() throws Exception {
                execute(grid(0), "BEGIN");

                return null;
            }
        }, IgniteSQLException.class, "MVCC must be enabled in order to start transaction.");
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testCommitWithMvccDisabled() throws Exception {
        execute(grid(0), "COMMIT");
        // assert no exception
    }

    /**
     * @throws Exception if failed.
     */
    @Test
    public void testRollbackWithMvccDisabled() throws Exception {
        execute(grid(0), "ROLLBACK");
        // assert no exception
    }
}
