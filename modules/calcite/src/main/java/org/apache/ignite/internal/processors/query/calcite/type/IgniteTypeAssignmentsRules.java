/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.type;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.UncheckedExecutionException;
import org.apache.calcite.sql.type.SqlTypeAssignmentRule;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeMappingRule;
import org.apache.calcite.sql.type.SqlTypeMappingRules;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Util;

import static org.apache.calcite.sql.type.SqlTypeName.APPROX_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.DAY_INTERVAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.EXACT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.FRACTIONAL_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_HOUR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_DAY_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_HOUR_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MINUTE_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_SECOND;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR;
import static org.apache.calcite.sql.type.SqlTypeName.INTERVAL_YEAR_MONTH;
import static org.apache.calcite.sql.type.SqlTypeName.INT_TYPES;
import static org.apache.calcite.sql.type.SqlTypeName.YEAR_INTERVAL_TYPES;

/**
 * Sets rules to explicit CAST. Calcite's {@link SqlTypeCoercionRule} and {@link SqlTypeAssignmentRule} do not satisfy
 * SQL standard.
 */
public class IgniteTypeAssignmentsRules implements SqlTypeMappingRule {
    /** */
    public static final IgniteTypeAssignmentsRules INSTANCE;

    static {
        IgniteTypeAssignmentsRules.Builder rules = new Builder();

        Set<SqlTypeName> rule = EnumSet.noneOf(SqlTypeName.class);

        // MULTISET is assignable from...
        rules.add(SqlTypeName.MULTISET, EnumSet.of(SqlTypeName.MULTISET));

        rule.clear();
        rule.addAll(EXACT_TYPES);
        rule.addAll(FRACTIONAL_TYPES);
        rule.addAll(CHAR_TYPES);

        // FLOAT (up to 64 bit floating point) is assignable from...
        // REAL (32 bit floating point) is assignable from...
        // DOUBLE is assignable from...
        // DECIMAL is assignable from...
        for (SqlTypeName type : FRACTIONAL_TYPES)
            rules.add(type, rule);

        rule.add(INTERVAL_YEAR);
        rule.add(INTERVAL_MONTH);
        rule.add(INTERVAL_DAY);
        rule.add(INTERVAL_HOUR);
        rule.add(INTERVAL_MINUTE);
        rule.add(INTERVAL_SECOND);

        // TINYINT is assignable from...
        // SMALLINT is assignable from...
        // INTEGER is assignable from...
        // BIGINT is assignable from...
        for (SqlTypeName type : EXACT_TYPES)
            rules.add(type, rule);

        // BINARY, VARBINARY is assignable from...
        rule.clear();
        rule.addAll(BINARY_TYPES);
        rule.addAll(CHAR_TYPES);
        for (SqlTypeName type : BINARY_TYPES)
            rules.add(type, rule);

        // CHAR is assignable from...
        // VARCHAR is assignable from...
        rule.clear();
        rule.addAll(CHAR_TYPES);
        rule.addAll(EXACT_TYPES);
        rule.addAll(APPROX_TYPES);
        rule.addAll(DAY_INTERVAL_TYPES);
        rule.addAll(YEAR_INTERVAL_TYPES);
        rule.add(BOOLEAN);
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.TIMESTAMP);
        rule.addAll(BINARY_TYPES);

        rules.add(SqlTypeName.CHAR, rule);
        rules.add(SqlTypeName.VARCHAR, rule);

        // BOOLEAN is assignable from...
        rule.clear();
        rule.addAll(INT_TYPES);
        rule.addAll(CHAR_TYPES);
        rule.add(BOOLEAN);
        rules.add(BOOLEAN, rule);

        // DATE is assignable from...
        rule.clear();
        rule.add(SqlTypeName.DATE);
        rule.addAll(CHAR_TYPES);
        rule.add(SqlTypeName.TIMESTAMP);
        rules.add(SqlTypeName.DATE, rule);

        // TIME is assignable from...
        rule.clear();
        rule.add(SqlTypeName.TIME);
        rule.addAll(CHAR_TYPES);
        rule.add(SqlTypeName.TIMESTAMP);
        rules.add(SqlTypeName.TIME, rule);

        // TIMESTAMP is assignable from ...
        rule.clear();
        rule.add(SqlTypeName.TIMESTAMP);
        rule.addAll(CHAR_TYPES);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.DATE);
        rules.add(SqlTypeName.TIMESTAMP, rule);

        // TIMESTAMP WITH LOCAL TIME ZONE is assignable from...
        rule.clear();
        rule.add(SqlTypeName.TIMESTAMP);
        rule.addAll(CHAR_TYPES);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.DATE);

        // GEOMETRY is assignable from ...
        rule.clear();
        rule.add(SqlTypeName.GEOMETRY);
        rule.addAll(CHAR_TYPES);
        rules.add(SqlTypeName.GEOMETRY, rule);

        rule.clear();
        rule.addAll(CHAR_TYPES);
        rule.addAll(YEAR_INTERVAL_TYPES);

        // IntervalYearMonth is assignable from...
        rules.add(INTERVAL_YEAR_MONTH, rule);

        rule.clear();
        rule.addAll(CHAR_TYPES);
        rule.addAll(DAY_INTERVAL_TYPES);

        List<SqlTypeName> multiIntervals = List.of(INTERVAL_DAY_HOUR, INTERVAL_DAY_MINUTE, INTERVAL_DAY_SECOND,
            INTERVAL_HOUR_MINUTE, INTERVAL_HOUR_SECOND, INTERVAL_MINUTE_SECOND);

        // IntervalDayHourMinuteSecond is assignable from...
        for (SqlTypeName type : multiIntervals)
            rules.add(type, rule);

        rule.clear();
        rule.addAll(CHAR_TYPES);
        rule.addAll(EXACT_TYPES);
        rule.addAll(YEAR_INTERVAL_TYPES);

        List<SqlTypeName> singleYearIntervals = List.of(INTERVAL_YEAR, INTERVAL_MONTH);

        for (SqlTypeName type : singleYearIntervals)
            rules.add(type, rule);

        rule.removeAll(YEAR_INTERVAL_TYPES);
        rule.addAll(DAY_INTERVAL_TYPES);

        List<SqlTypeName> singleDayIntervals = List.of(INTERVAL_DAY, INTERVAL_HOUR, INTERVAL_MINUTE, INTERVAL_SECOND);

        for (SqlTypeName type : singleDayIntervals)
            rules.add(type, rule);

        // ARRAY is assignable from ...
        rules.add(SqlTypeName.ARRAY, EnumSet.of(SqlTypeName.ARRAY));

        // MAP is assignable from ...
        rules.add(SqlTypeName.MAP, EnumSet.of(SqlTypeName.MAP));

        // SYMBOL is assignable from ...
        rules.add(SqlTypeName.SYMBOL, EnumSet.of(SqlTypeName.SYMBOL));

        // ANY is assignable from ...
        rule.clear();
        rule.add(SqlTypeName.TINYINT);
        rule.add(SqlTypeName.SMALLINT);
        rule.add(SqlTypeName.INTEGER);
        rule.add(SqlTypeName.BIGINT);
        rule.add(SqlTypeName.DECIMAL);
        rule.add(SqlTypeName.FLOAT);
        rule.add(SqlTypeName.REAL);
        rule.add(SqlTypeName.TIME);
        rule.add(SqlTypeName.DATE);
        rule.add(SqlTypeName.TIMESTAMP);
        rules.add(SqlTypeName.ANY, rule);

        INSTANCE = new IgniteTypeAssignmentsRules(rules.map);
    }

    /** */
    private final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;

    /** */
    private IgniteTypeAssignmentsRules(Map<SqlTypeName, ImmutableSet<SqlTypeName>> map) {
        this.map = ImmutableMap.copyOf(map);
    }

    /** {@inheritDoc} */
    @Override public Map<SqlTypeName, ImmutableSet<SqlTypeName>> getTypeMapping() {
        return map;
    }

    /** Keeps state while building the type mappings. */
    private static class Builder {
        /** */
        final Map<SqlTypeName, ImmutableSet<SqlTypeName>> map;

        /** */
        final LoadingCache<Set<SqlTypeName>, ImmutableSet<SqlTypeName>> sets;

        /** Creates an empty {@link SqlTypeMappingRules.Builder}. */
        private Builder() {
            map = new EnumMap<>(SqlTypeName.class);

            sets = CacheBuilder.newBuilder().build(CacheLoader.from(Sets::immutableEnumSet));
        }

        /** Add a map entry to the existing {@link SqlTypeMappingRules.Builder} mapping. */
        void add(SqlTypeName fromType, Set<SqlTypeName> toTypes) {
            try {
                map.put(fromType, sets.get(toTypes));
            }
            catch (UncheckedExecutionException | ExecutionException e) {
                throw Util.throwAsRuntime("populating SqlTypeAssignmentRules", Util.causeOrSelf(e));
            }
        }
    }
}
