/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.processors.query.calcite.fun;

import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Strict;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Ignite SQL functions.
 */
public class IgniteSqlFunctions {
    /**
     * Default constructor.
     */
    private IgniteSqlFunctions() {
        // No-op.
    }

    /** SQL SYSTEM_RANGE(start, end) table function. */
    public static ScannableTable system_range(Long rangeStart, Long rangeEnd) {
        return system_range(rangeStart, rangeEnd, 1);
    }

    /** SQL SYSTEM_RANGE(start, end, increment) table function. */
    public static ScannableTable system_range(Long rangeStart, Long rangeEnd, long increment) {
        return new RangeTable(rangeStart, rangeEnd, increment);
    }

    /** SQL LENGTH(string) function. */
    @Strict
    public static int length(String str) {
        return str.length();
    }

    /** */
    private static class RangeTable implements ScannableTable {
        /** Start of the range. */
        private final Long rangeStart;

        /** End of the range. */
        private final Long rangeEnd;

        /** Increment. */
        private final Long increment;

        /** */
        RangeTable(Long rangeStart, Long rangeEnd, Long increment) {
            this.rangeStart = rangeStart;
            this.rangeEnd = rangeEnd;
            this.increment = increment;
        }

        /** {@inheritDoc} */
        @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory.builder().add("X", SqlTypeName.BIGINT).build();
        }

        /** {@inheritDoc} */
        @Override public Enumerable<@Nullable Object[]> scan(DataContext root) {
            if (rangeStart == null || rangeEnd == null || increment == null)
                return Linq4j.emptyEnumerable();

            if (increment == 0L)
                throw new IllegalArgumentException("Increment can't be 0");

            return new AbstractEnumerable<@Nullable Object[]>() {
                @Override public Enumerator<@Nullable Object[]> enumerator() {
                    return new Enumerator<Object[]>() {
                        long cur = rangeStart - increment;

                        @Override public Object[] current() {
                            return new Object[] { cur };
                        }

                        @Override public boolean moveNext() {
                            cur += increment;

                            return increment > 0L ? cur <= rangeEnd : cur >= rangeEnd;
                        }

                        @Override public void reset() {
                            cur = rangeStart - increment;
                        }

                        @Override public void close() {
                            // No-op.
                        }
                    };
                }
            };
        }

        /** {@inheritDoc} */
        @Override public Statistic getStatistic() {
            // We can't get access to this method from physical node on planning phase and Calcite dosn't use it either,
            // so we can return any value here, it can't be used.
            return Statistics.UNKNOWN;
        }

        /** {@inheritDoc} */
        @Override public Schema.TableType getJdbcTableType() {
            return Schema.TableType.TABLE;
        }

        /** {@inheritDoc} */
        @Override public boolean isRolledUp(String column) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean rolledUpColumnValidInsideAgg(String column, SqlCall call,
            SqlNode parent, CalciteConnectionConfig cfg) {
            return true;
        }
    }
}
