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

import java.sql.PreparedStatement;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class PreparedStatementExSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testStoringMeta() throws Exception {
        PreparedStatement stmt = stmt();

        PreparedStatementEx wrapped = stmt.unwrap(PreparedStatementEx.class);

        wrapped.putMeta(0, "0");

        assertEquals("0", wrapped.meta(0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testStoringMoreMetaKeepsExisting() throws Exception {
        PreparedStatement stmt = stmt();

        PreparedStatementEx wrapped = stmt.unwrap(PreparedStatementEx.class);

        wrapped.putMeta(0, "0");
        wrapped.putMeta(1, "1");

        assertEquals("0", wrapped.meta(0));
        assertEquals("1", wrapped.meta(1));
    }

    /**
     *
     */
    private static PreparedStatement stmt() {
        return new PreparedStatementExImpl(null);
    }
}