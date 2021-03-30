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

import static org.apache.ignite.internal.metastorage.server.ValueCondition.Type.EQUAL;
import static org.apache.ignite.internal.metastorage.server.ValueCondition.Type.NOT_EQUAL;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for entry value condition.
 *
 * @see ValueCondition
 */
public class ValueConditionTest {
    /** Entry key. */
    private static final byte[] KEY = new byte[] {1};

    /** Entry value. */
    private static final byte[] VAL_1 = new byte[] {11};

    /** Other entry value. */
    private static final byte[] VAL_2 = new byte[] {22};

    /**
     * Tests values equality.
     */
    @Test
    public void eq() {
        Condition cond = new ValueCondition(EQUAL, KEY, VAL_1);

        assertTrue(cond.test(new Entry(KEY, VAL_1, 1, 1)));
    }

    /**
     * Tests values inequality.
     */
    @Test
    public void ne() {
        Condition cond = new ValueCondition(NOT_EQUAL, KEY, VAL_1);

        assertTrue(cond.test(new Entry(KEY, VAL_2, 1, 1)));
    }
}
