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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class StatementWithMetaSelfTest extends GridCommonAbstractTest {
    public void testStoringMeta() {
        StatementWithMeta stmt = stmt();
        stmt.putMeta(0, "0");

        assertEquals("0", stmt.meta(0));
    }

    public void testStoringMoreMetaKeepsExisting() {
        StatementWithMeta stmt = stmt();
        stmt.putMeta(0, "0");
        stmt.putMeta(1, "1");

        assertEquals("0", stmt.meta(0));
        assertEquals("1", stmt.meta(1));
    }

    private static StatementWithMeta stmt() {
        return new StatementWithMeta(null);
    }
}