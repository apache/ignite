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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;

import org.apache.ignite.Ignite;

/**
 * Common parts to test statistics collection for different types.
 */
public abstract class StatisticsTypesAbstractTest extends StatisticsAbstractTest {
    /** Types to test. */
    protected static final String TYPES[] = new String[]{"BOOLEAN", "INT", "TINYINT", "SMALLINT", "BIGINT",
        "DECIMAL", "DOUBLE", "REAL", "TIME", "DATE", "TIMESTAMP", "VARCHAR", "CHAR", "UUID", "BINARY", "GEOMETRY"};

    /** Column names prefix. */
    protected static final String COL_NAME_PREFIX = "COL_";

    /** Start date. */
    private static final String START_DATE = "1970.01.01 12:00:00 UTC";

    /** Start time. */
    protected static final long TIMESTART;

    /** Time format. */
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("HH:mm:ss");

    /** Date format. */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    /** Timestam format. */
    private static final SimpleDateFormat TIMESTAMP_FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    static {
        TimeZone tz = TimeZone.getTimeZone("UTC");
        SimpleDateFormat SDF = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss z");
        SDF.setTimeZone(tz);
        Calendar cal = Calendar.getInstance();
        try {
            cal.setTime(SDF.parse(START_DATE));
        }
        catch (ParseException e) {
            // No-op.
        }
        TIMESTART = cal.getTimeInMillis();

        TIME_FORMATTER.setTimeZone(tz);
        DATE_FORMATTER.setTimeZone(tz);
        TIMESTAMP_FORMATTER.setTimeZone(tz);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite node = startGridsMultiThreaded(1);

        node.getOrCreateCache(DEFAULT_CACHE_NAME);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        sql("DROP TABLE IF EXISTS dtypes");

        StringBuilder create = new StringBuilder("CREATE TABLE dtypes (ID INT PRIMARY KEY, col_index int, col_no_index int");
        for (String type : TYPES)
            create.append(", ").append(COL_NAME_PREFIX).append(type).append(" ").append(type);

        create.append(")");

        sql(create.toString());

        sql("CREATE INDEX dtypes_col_index ON dtypes(col_index)");
        for (String type : TYPES)
            sql(String.format("CREATE INDEX dtypes_%s ON dtypes(col_%s)", type, type));

        for (int i = 1; i < SMALL_SIZE; i++)
            sql(insert(i));

        for (int i = 0; i > -SMALL_SIZE / 2; i--)
            sql(insertNulls(i));

        collectStatistics("dtypes");
    }

    /**
     * Generate unique value by type and counter.
     *
     * @param type Type.
     * @param cntr Counter.
     * @return Generated value.
     */
    private String getVal(String type, long cntr) {

        switch (type) {
            case "BOOLEAN":
                return ((cntr & 1) == 0) ? "False" : "True";

            case "INT":
                return String.valueOf(cntr % 2147483648L);

            case "TINYINT":
                return String.valueOf(cntr % 128);

            case "SMALLINT":
                return String.valueOf(cntr % 32768);

            case "BIGINT":
                return String.valueOf(cntr);

            case "DECIMAL":
            case "DOUBLE":
            case "REAL":
                return String.valueOf((double) cntr / 100);

            case "TIME":
                Calendar timeCalendar = Calendar.getInstance();
                timeCalendar.setTimeInMillis(TIMESTART);
                timeCalendar.add(Calendar.SECOND, (int) cntr);
                return "'" + TIME_FORMATTER.format(timeCalendar.getTime()) + "'";

            case "DATE":
                Calendar dateCalendar = Calendar.getInstance();
                dateCalendar.setTimeInMillis(TIMESTART);
                dateCalendar.add(Calendar.DATE, (int) cntr);
                return "'" + DATE_FORMATTER.format(dateCalendar.getTime()) + "'";

            case "TIMESTAMP":
                Calendar tsCalendar = Calendar.getInstance();
                tsCalendar.setTimeInMillis(TIMESTART);
                tsCalendar.add(Calendar.SECOND, (int) cntr);
                return "'" + TIMESTAMP_FORMATTER.format(tsCalendar.getTime()) + "'";

            case "VARCHAR":
                return "'varchar" + cntr + "'";

            case "CHAR":
                return "'" + (char)((int)'A' + cntr % 26) + "'";

            case "UUID":
                return "'" + new UUID(0L, cntr) + "'";

            case "BINARY":
                return String.valueOf(cntr);

            case "GEOMETRY":
                return "null";

            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Generate insert SQL command by counter value.
     *
     * @param cntr Counter value to generate by.
     * @return Insert into dtypes command.
     */
    private String insertNulls(long cntr) {
        return String.format("INSERT INTO dtypes(id) values (%d)", cntr);
    }

    /**
     * Build insert SQL command for single row by counter.
     *
     * @param cntr Counter.
     * @return Insert SQL.
     */
    private String insert(long cntr) {
        StringBuilder insert = new StringBuilder("INSERT INTO dtypes(id, col_index, col_no_index");

        for (String type : TYPES)
            insert.append(", col_").append(type);

        insert.append(") VALUES (")
            .append(cntr).append(", ")
            .append(cntr).append(", ")
            .append(cntr);

        for (String type : TYPES)
            insert.append(", ").append(getVal(type, cntr));

        insert.append(")");

        return insert.toString();
    }
}
