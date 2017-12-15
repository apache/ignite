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

package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL column.
 */
public class SqlColumn {
    /** Column name. */
    private final String name;

    /** Column type. */
    private final SqlColumnType typ;

    /** Scale. */
    private final int scale;

    /** Precision. */
    private final int precision;

    /** Is column nullable. */
    private final boolean isNullable;

    /**
     * Constructs the object.
     *
     * @param name Name.
     * @param typ Type.
     */
    public SqlColumn(String name, SqlColumnType typ) {
        this(name, typ, 0, 0, true);
    }

    /**
     * Constructs the object.
     *
     * @param name Name.
     * @param typ Type.
     * @param precision Precision.
     */
    public SqlColumn(String name, SqlColumnType typ, int precision) {
        this(name, typ, 0, precision, true);
    }

    /**
     * Constructs the object.
     *
     * @param name Name.
     * @param typ Type.
     * @param precision Precision.
     * @param scale Scale.
     * @param isNullable Is nullable.
     */
    public SqlColumn(String name, SqlColumnType typ, int scale, int precision, boolean isNullable) {
        this.name = name;
        this.typ = typ;
        this.scale = scale;
        this.precision = precision;
        this.isNullable = isNullable;
    }

    /**
     * @return Name.
     */
    public String name() {
        return name;
    }

    /**
     * @return Type.
     */
    public SqlColumnType type() {
        return typ;
    }

    /**
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * @return Precision.
     */
    public int precision() {
        return precision;
    }

    /**
     * Returns true if column is nullable.
     *
     * @return true if column is nullable.
     */
    public boolean isNullable() {
        return isNullable;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlColumn.class, this);
    }
}
