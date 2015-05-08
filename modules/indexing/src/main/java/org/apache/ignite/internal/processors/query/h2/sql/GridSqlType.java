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

/**
 * SQL Data type based on H2.
 */
public class GridSqlType {
    /** H2 type. */
    private int type;

    /** */
    private int scale;

    /** */
    private long precision;

    /** */
    private int displaySize;

    /** */
    private String sql;

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
}
