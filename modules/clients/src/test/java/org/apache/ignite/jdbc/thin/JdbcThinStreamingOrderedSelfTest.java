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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;

/**
 * Tests for ordered streaming via thin driver.
 */
public class JdbcThinStreamingOrderedSelfTest extends JdbcThinStreamingAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection createStreamedConnection(boolean allowOverwrite, long flushFreq) throws Exception {
        Connection c = connect(grid(0), null);

        execute(c, "SET STREAMING 1 BATCH_SIZE " + batchSize
            + " ALLOW_OVERWRITE " + (allowOverwrite ? 1 : 0)
            + " PER_NODE_BUFFER_SIZE 1000 "
            + " FLUSH_FREQUENCY " + flushFreq
            + " ORDERED;"
        );

        return c;
    }
}