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
import org.apache.ignite.internal.jdbc2.JdbcStreamingSelfTest;

/**
 * Tests for streaming via thin driver.
 */
public class JdbcThinStreamingSelfTest extends JdbcStreamingSelfTest {
    /** {@inheritDoc} */
    @Override protected Connection createConnection(boolean allowOverwrite) throws Exception {
        Connection res = JdbcThinAbstractSelfTest.connect(grid(0), "streaming=true&streamingFlushFrequency=500&" +
            "streamingAllowOverwrite=" + allowOverwrite);

        res.setSchema('"' + DEFAULT_CACHE_NAME + '"');

        return res;
    }
}