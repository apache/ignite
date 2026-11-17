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
package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.internal.processors.query.calcite.QueryChecker.containsSubPlan;

/** */
public class DecimalHashJoinIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** */
    @Test
    public void testEqualDecimalKeysWithDifferentScales() {
        sql("CREATE TABLE LEFT_ROWS (id INT PRIMARY KEY, k DECIMAL)");
        sql("CREATE TABLE RIGHT_ROWS (id INT PRIMARY KEY, k DECIMAL)");

        sql("INSERT INTO LEFT_ROWS (id, k) VALUES (1, ?)", new BigDecimal("11845600"));
        sql("INSERT INTO RIGHT_ROWS (id, k) VALUES (1, ?)", new BigDecimal("1.18456E+7"));

        BigDecimal leftRowsVal = (BigDecimal)sql("SELECT k FROM LEFT_ROWS WHERE id = 1").get(0).get(0);
        BigDecimal rightRowsVal = (BigDecimal)sql("SELECT k FROM RIGHT_ROWS WHERE id = 1").get(0).get(0);

        assertEquals(0, leftRowsVal.compareTo(rightRowsVal));
        assertEquals(0, leftRowsVal.scale());
        assertEquals(-2, rightRowsVal.scale());

        assertQuery("SELECT /*+ HASH_JOIN */ l.k, r.k FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k WHERE l.k = 11845600")
            .matches(containsSubPlan("IgniteHashJoin"))
            .resultSize(1)
            .check();

        assertQuery("SELECT l.k, r.k FROM LEFT_ROWS l JOIN RIGHT_ROWS r ON l.k = r.k WHERE l.k = 11845600")
            .resultSize(1)
            .check();
    }
}
