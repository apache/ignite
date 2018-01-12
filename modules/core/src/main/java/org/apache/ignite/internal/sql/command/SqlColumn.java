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
import org.jetbrains.annotations.Nullable;

/**
 * SQL column.
 */
public class SqlColumn {
    /** Column name. */
    private String name;

    /** Column type. */
    private SqlColumnType typ;

    /** Scale. */
    private @Nullable Integer scale;

    /** Precision. */
    private @Nullable Integer precision;

    /** Is column nullable. */
    private @Nullable Boolean isNullable;

    /**
     * Constructs the object.
     *
     * @param name Name.
     * @param typ Type.
     */
    public SqlColumn(String name, SqlColumnType typ) {
        this(name, typ, null, null, null);
    }

    /**
     * Constructs the object.
     *
     * @param name Name.
     * @param typ Type.
     * @param length Precision.
     */
    public SqlColumn(String name, SqlColumnType typ, @Nullable Integer length) {
        this(name, typ, null, length, null);
    }

    /**
     * Constructs the object.
     *
     * @param name Name.
     * @param typ Type.
     * @param precision Precision.
     */
    public SqlColumn(String name, SqlColumnType typ, @Nullable Integer precision, @Nullable Boolean isNullable) {
        this(name, typ, null, precision, isNullable);
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
    public SqlColumn(String name, SqlColumnType typ, @Nullable Integer scale, @Nullable Integer precision,
        Boolean isNullable) {

        this.name = name;
        this.typ = typ;
        this.scale = scale;
        this.precision = precision;
        this.isNullable = isNullable;
    }

    /**
     * Returns column name.
     * @return The column name.
     */
    public String name() {
        return name;
    }

    /**
     * Sets the column name.
     * @param name The column name.
     */
    public void name(String name) {
        this.name = name;
    }

    /**
     * Returns column type.
     * @return The column type.
     */
    public SqlColumnType type() {
        return typ;
    }

    /**
     * Sets the column type.
     * @param typ The column type.
     */
    public void typ(SqlColumnType typ) {
        this.typ = typ;
    }

    /**
     * Returns the scale.
     * @return The scale.
     */
    public @Nullable Integer scale() {
        return scale;
    }

    /**
     * Sets the scale.
     * @param scale The scale.
     */
    public void scale(@Nullable Integer scale) {
        this.scale = scale;
    }

    /**
     * Returns the precision.
     * @return precision.
     */
    public @Nullable Integer precision() {
        return precision;
    }

    /**
     * Sets the precision.
     * @param precision The precision.
     */
    public void precision(@Nullable Integer precision) {
        this.precision = precision;
    }

    /**
     * Returns true if column is nullable.
     * @return true if column is nullable.
     */
    public @Nullable Boolean isNullable() {
        return isNullable;
    }

    /**
     * Sets the nullable flag.
     * @param isNullable The nullable flag.
     */
    public void isNullable(@Nullable Boolean isNullable) {
        isNullable = isNullable;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(SqlColumn.class, this);
    }
}
