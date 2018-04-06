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

package org.apache.ignite.jdbc.thin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.store.jdbc.model.Person;
import org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest;
import org.apache.ignite.internal.processors.query.GridQueryProcessor;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;

/**
 * Tests for streaming via thin driver.
 */
public class JdbcThinStreamingNotOrderedSelfTest extends JdbcThinStreamingSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection createStreamedConnection(boolean allowOverwrite, long flushFreq) throws Exception {
        return createStreamedConnection(allowOverwrite, flushFreq, false);
    }

    /**
     * @param allowOverwrite Allow overwriting of existing keys.
     * @param flushFreq Stream flush timeout.
     * @param ordered Ordered flag.
     * @return Connection to use for the test.
     * @throws Exception if failed.
     */
    private Connection createStreamedConnection(boolean allowOverwrite, long flushFreq,
        boolean ordered) throws Exception {
        Connection c = JdbcThinAbstractSelfTest.connect(grid(0), null);

        execute(c, "SET STREAMING 1 BATCH_SIZE " + batchSize
            + " ALLOW_OVERWRITE " + (allowOverwrite ? 1 : 0)
            + " PER_NODE_BUFFER_SIZE 1000 "
            + " FLUSH_FREQUENCY " + flushFreq
            + " ORDERED " +  (ordered ? "ON" : "OFF")
        );

        return c;
    }
}