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

package org.apache.ignite.internal.jdbc2;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Test to check JDBC2 driver behavior when cache specified in connection string does not have any query entities.
 */
public class JdbcDefaultNoOpCacheTest extends org.apache.ignite.jdbc.JdbcDefaultNoOpCacheTest {
    /** Ignite configuration URL. */
    private static final String CFG_URL = "modules/clients/src/test/config/jdbc-config.xml";

    /** {@inheritDoc} */
    protected String getUrl() {
        return CFG_URL_PREFIX + "cache=noop@" + CFG_URL;
    }
}
