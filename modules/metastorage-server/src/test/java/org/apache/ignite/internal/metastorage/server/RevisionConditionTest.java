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

package org.apache.ignite.internal.metastorage.server;

import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.metastorage.server.RevisionCondition.Type.EQUAL;
import static org.apache.ignite.internal.metastorage.server.RevisionCondition.Type.GREATER;
import static org.apache.ignite.internal.metastorage.server.RevisionCondition.Type.GREATER_OR_EQUAL;
import static org.apache.ignite.internal.metastorage.server.RevisionCondition.Type.LESS;
import static org.apache.ignite.internal.metastorage.server.RevisionCondition.Type.LESS_OR_EQUAL;
import static org.apache.ignite.internal.metastorage.server.RevisionCondition.Type.NOT_EQUAL;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for entry revision condition.
 *
 * @see RevisionCondition
 */
public class RevisionConditionTest {
    /** Entry key. */
    private static final byte[] KEY = new byte[] {1};

    /** Entry value. */
    private static final byte[] VAL = new byte[] {2};

    /**
     * Tests revisions equality.
     */
    @Test
    public void eq() {
        Condition cond = new RevisionCondition(EQUAL, KEY, 1);

        // 1 == 1.
        assertTrue(cond.test(new Entry(KEY, VAL, 1, 1)));
    }

    /**
     * Tests revisions inequality.
     */
    @Test
    public void ne() {
        Condition cond = new RevisionCondition(NOT_EQUAL, KEY, 1);

        // 2 != 1.
        assertTrue(cond.test(new Entry(KEY, VAL, 2, 1)));
    }

    /**
     * Tests that revision is greater than another one.
     */
    @Test
    public void gt() {
        Condition cond = new RevisionCondition(GREATER, KEY, 1);

        // 2 > 1.
        assertTrue(cond.test(new Entry(KEY, VAL, 2, 1)));
    }

    /**
     * Tests that revision is greater than or equal to another one .
     */
    @Test
    public void ge() {
        Condition cond = new RevisionCondition(GREATER_OR_EQUAL, KEY, 1);

        // 2 >= 1 (2 > 1).
        assertTrue(cond.test(new Entry(KEY, VAL, 2, 1)));

        // 1 >= 1 (1 == 1).
        assertTrue(cond.test(new Entry(KEY, VAL, 1, 1)));
    }

    /**
     * Tests that revision is less than another one.
     */
    @Test
    public void lt() {
        Condition cond = new RevisionCondition(LESS, KEY, 2);

        // 1 < 2
        assertTrue(cond.test(new Entry(KEY, VAL, 1, 1)));
    }

    /**
     * Tests that revision is less than or equal to another one .
     */
    @Test
    public void le() {
        Condition cond = new RevisionCondition(LESS_OR_EQUAL, KEY, 2);

        // 1 <= 2 (1 < 2)
        assertTrue(cond.test(new Entry(KEY, VAL, 1, 1)));

        // 1 <= 1 (1 == 1).
        assertTrue(cond.test(new Entry(KEY, VAL, 1, 1)));
    }
}
