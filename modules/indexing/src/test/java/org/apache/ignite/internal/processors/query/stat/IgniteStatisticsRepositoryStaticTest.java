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

package org.apache.ignite.internal.processors.query.stat;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

/**
 * Test of static methods of Ignite statistics repository.
 */
public class IgniteStatisticsRepositoryStaticTest extends StatisticsAbstractTest {
    /** First default key. */
    protected static final StatisticsKey K1 = new StatisticsKey(SCHEMA, "tab1");

    /** Second default key. */
    protected static final StatisticsKey K2 = new StatisticsKey(SCHEMA, "tab2");

    /** Column statistics with 100 nulls. */
    protected ColumnStatistics cs1 = new ColumnStatistics(null, null, 100, 0, 100,
        0, new byte[0], 0, U.currentTimeMillis());

    /** Column statistics with 100 integers 0-100. */
    protected ColumnStatistics cs2 = new ColumnStatistics(new BigDecimal(0), new BigDecimal(100), 0, 100, 100,
        4, new byte[0], 0, U.currentTimeMillis());

    /** Column statistics with 0 rows. */
    protected ColumnStatistics cs3 = new ColumnStatistics(null, null, 0, 0, 0, 0,
        new byte[0], 0, U.currentTimeMillis());

    /** Column statistics with 100 integers 0-10. */
    protected ColumnStatistics cs4 = new ColumnStatistics(new BigDecimal(0), new BigDecimal(10), 0, 10, 100,
        4, new byte[0], 0, U.currentTimeMillis());

    /**
     * 1) Remove not existing column.
     * 2) Remove some columns.
     * 3) Remove all columns.
     */
    @Test
    public void subtractTest() {
        HashMap<String, ColumnStatistics> colStat1 = new HashMap<>();
        colStat1.put("col1", cs1);
        colStat1.put("col2", cs2);

        ObjectStatisticsImpl os = new ObjectStatisticsImpl(100, colStat1);

        // 1) Remove not existing column.
        ObjectStatisticsImpl os1 = os.subtract(Collections.singleton("col0"));

        assertEquals(os, os1);

        // 2) Remove some columns.
        ObjectStatisticsImpl os2 = os.subtract(Collections.singleton("col1"));

        assertEquals(1, os2.columnsStatistics().size());
        assertEquals(cs2, os2.columnStatistics("col2"));

        // 3) Remove all columns.
        ObjectStatisticsImpl os3 = os.subtract(Arrays.stream(new String[] {"col2", "col1"}).collect(Collectors.toSet()));

        assertTrue(os3.columnsStatistics().isEmpty());
    }
}
