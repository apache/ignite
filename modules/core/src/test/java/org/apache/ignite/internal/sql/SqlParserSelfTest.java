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

package org.apache.ignite.internal.sql;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Test for parser.
 */
public class SqlParserSelfTest extends GridCommonAbstractTest {
    /**
     * Tests for CREATE INDEX command.
     *
     * @throws Exception If failed.
     */
    public void testCreateIndex() throws Exception {
        System.out.println(new SqlParser(null, "CREATE TABLE t (a VARCHAR, b TINYINT, PRIMARY KEY (a))").nextCommand());
    }
}
