/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples;

import org.apache.ignite.examples.sql.SqlDdlExample;
import org.apache.ignite.examples.sql.SqlDmlExample;
import org.apache.ignite.examples.sql.SqlQueriesExample;
import org.apache.ignite.testframework.junits.common.GridAbstractExamplesTest;
import org.junit.Test;

/**
 * SQL examples self test.
 */
public class SqlExamplesSelfTest extends GridAbstractExamplesTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlJavaExample() throws Exception {
        SqlQueriesExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlDmlExample() throws Exception {
        SqlDmlExample.main(EMPTY_ARGS);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSqlDdlExample() throws Exception {
        SqlDdlExample.main(EMPTY_ARGS);
    }
}
