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

package org.apache.ignite.internal.processors.query.h2.sql;

import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.value.Value;
import org.h2.value.ValueDouble;
import org.h2.value.ValueLong;

/**
 * SQL Data type based on H2.
 */
public final class GridSqlType {
    /** */
    public static final GridSqlType UNKNOWN = new GridSqlType(Value.UNKNOWN, 0, 0, 0, null);

    /** */
    public static final GridSqlType BIGINT = new GridSqlType(Value.LONG, 0, ValueLong.PRECISION,
        ValueLong.DISPLAY_SIZE, "BIGINT");

    /** */
    public static final GridSqlType DOUBLE = new GridSqlType(Value.DOUBLE, 0, ValueDouble.PRECISION,
        ValueDouble.DISPLAY_SIZE, "DOUBLE");

    /** */
    public static final GridSqlType UUID = new GridSqlType(Value.UUID, 0, Integer.MAX_VALUE, 36, "UUID");

    /** H2 type. */
    private final int type;

    /** */
    private final int scale;

    /** */
    private final long precision;

    /** */
    private final int displaySize;

    /** */
    private final String sql;

    /**
     * @param type H2 Type.
     * @param scale Scale.
     * @param precision Precision.
     * @param displaySize Display size.
     * @param sql SQL definition of the type.
     */
    public GridSqlType(int type, int scale, long precision, int displaySize, String sql) {
        this.type = type;
        this.scale = scale;
        this.precision = precision;
        this.displaySize = displaySize;
        this.sql = sql;
    }

    /**
     * @return Get H2 type.
     */
    public int type() {
        return type;
    }

    /**
     * Get the scale of this expression.
     *
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * Get the precision of this expression.
     *
     * @return Precision.
     */
    public long precision() {
        return precision;
    }

    /**
     * Get the display size of this expression.
     *
     * @return the display size
     */
    public int displaySize() {
        return displaySize;
    }

    /**
     * @return SQL definition of the type.
     */
    public String sql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSqlType.class, this);
    }
}