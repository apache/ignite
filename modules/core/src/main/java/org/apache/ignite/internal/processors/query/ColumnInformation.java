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
 *
 */

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Information about table column.
 */
public class ColumnInformation {
    /** */
    private final int ordinalPosition;

    /** */
    private final String schemaName;

    /** */
    private final String tblName;

    /** */
    private final String colName;

    /** */
    private final Class<?> fieldCls;

    /** */
    private final boolean nullable;

    /** */
    private final Object dfltVal;

    /** */
    private final int precision;

    /** */
    private final int scale;

    /** */
    private final boolean affinityCol;

    /**
     * @param ordinalPosition Ordinal column position.
     * @param schemaName Schema name.
     * @param tblName Table name.
     * @param colName Column name.
     * @param fieldCls Field class.
     * @param nullable Nullable.
     * @param dfltVal Default value.
     * @param precision Precision for a column or -1 if not applicable.
     * @param scale Scale for a column or -1 if not applicable.
     */
    public ColumnInformation(int ordinalPosition, String schemaName, String tblName, String colName, Class<?> fieldCls,
        boolean nullable, Object dfltVal, int precision, int scale, boolean affinityCol) {
        this.ordinalPosition = ordinalPosition;
        this.schemaName = schemaName;
        this.tblName = tblName;
        this.colName = colName;
        this.fieldCls = fieldCls;
        this.nullable = nullable;
        this.dfltVal = dfltVal;
        this.precision = precision;
        this.scale = scale;
        this.affinityCol = affinityCol;
    }

    /**
     * @return Column id.
     */
    public int columnId() {
        return ordinalPosition;
    }

    /**
     * @return Schema name.
     */
    public String schemaName() {
        return schemaName;
    }

    /**
     * @return Table name.
     */
    public String tableName() {
        return tblName;
    }

    /**
     * @return Column name.
     */
    public String columnName() {
        return colName;
    }

    /**
     * @return Class of column type.
     */
    public Class<?> fieldClass() {
        return fieldCls;
    }

    /**
     * @return {@code true} For nullabe column
     */
    public boolean nullable() {
        return nullable;
    }

    /**
     * @return Default value for column or {@code null} in case dafault value wasn't set
     */
    public Object defaultValue() {
        return dfltVal;
    }

    /**
     * @return Precision.
     */
    public int precision() {
        return precision;
    }

    /**
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * @return {@code true} For affinity column.
     */
    public boolean affinityColumn() {
        return affinityCol;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        ColumnInformation info = (ColumnInformation)o;

        return F.eq(schemaName, info.schemaName) && F.eq(tblName, info.tblName) && F.eq(colName, info.colName);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = schemaName != null ? schemaName.hashCode() : 0;

        res = 31 * res + (tblName != null ? tblName.hashCode() : 0);

        res = 31 * res + colName.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ColumnInformation.class, this);
    }
}
