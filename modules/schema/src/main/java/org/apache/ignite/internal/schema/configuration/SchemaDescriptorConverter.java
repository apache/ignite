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

package org.apache.ignite.internal.schema.configuration;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaException;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.SchemaTable;

import static org.apache.ignite.internal.schema.NativeTypes.BYTE;
import static org.apache.ignite.internal.schema.NativeTypes.DOUBLE;
import static org.apache.ignite.internal.schema.NativeTypes.FLOAT;
import static org.apache.ignite.internal.schema.NativeTypes.INTEGER;
import static org.apache.ignite.internal.schema.NativeTypes.LONG;
import static org.apache.ignite.internal.schema.NativeTypes.SHORT;
import static org.apache.ignite.internal.schema.NativeTypes.UUID;

/**
 * Build SchemaDescriptor from SchemaTable internal configuration.
 */
public class SchemaDescriptorConverter {
    /**
     * Convert ColumnType to NativeType.
     *
     * @param colType ColumnType.
     * @return NativeType.
     */
    private static NativeType convert(ColumnType colType) {
        assert colType != null;

        ColumnType.ColumnTypeSpec type = colType.typeSpec();

        switch (type) {
            case INT8:
                return BYTE;

            case INT16:
                return SHORT;

            case INT32:
                return INTEGER;

            case INT64:
                return LONG;

            case UINT8:
            case UINT16:
            case UINT32:
            case UINT64:
                throw new UnsupportedOperationException("Unsigned types are not supported yet.");

            case FLOAT:
                return FLOAT;

            case DOUBLE:
                return DOUBLE;

            case DECIMAL:
                ColumnType.NumericColumnType numType = (ColumnType.NumericColumnType)colType;

                return NativeTypes.decimalOf(numType.precision(), numType.scale());
            case UUID:
                return UUID;

            case BITMASK:
                return NativeTypes.bitmaskOf(((ColumnType.VarLenColumnType)colType).length());

            case STRING:
                int strLen = ((ColumnType.VarLenColumnType)colType).length();

                if (strLen == 0)
                    strLen = Integer.MAX_VALUE;

                return NativeTypes.stringOf(strLen);

            case BLOB:
                int blobLen = ((ColumnType.VarLenColumnType)colType).length();

                if (blobLen == 0)
                    blobLen = Integer.MAX_VALUE;

                return NativeTypes.blobOf(blobLen);

            default:
                throw new InvalidTypeException("Unexpected type " + type);
        }
    }

    /**
     * Convert column from public configuration to internal.
     *
     * @param colCfg Column to confvert.
     * @return Internal Column.
     */
    private static Column convert(org.apache.ignite.schema.Column colCfg) {
        NativeType type = convert(colCfg.type());

        return new Column(colCfg.name(), type, colCfg.nullable(), new ConstantSupplier(convertDefault(type, (String)colCfg.defaultValue())));
    }

    /**
     * TODO: https://issues.apache.org/jira/browse/IGNITE-14479 Fix default conversion.
     *
     * @param type Column type.
     * @param dflt Column default value.
     * @return Parsed object.
     */
    private static Serializable convertDefault(NativeType type, String dflt) {
        if (dflt == null || dflt.isEmpty() && type.spec() != NativeTypeSpec.STRING)
            return null;

        assert dflt instanceof String;

        switch (type.spec()) {
            case BYTE:
                return Byte.parseByte(dflt);
            case SHORT:
                return Short.parseShort(dflt);
            case INTEGER:
                return Integer.parseInt(dflt);
            case LONG:
                return Long.parseLong(dflt);
            case FLOAT:
                return Float.parseFloat(dflt);
            case DOUBLE:
                return Double.parseDouble(dflt);
            case DECIMAL:
                return new BigDecimal(dflt);
            case STRING:
                return dflt;
            case UUID:
                return java.util.UUID.fromString(dflt);
            default:
                throw new SchemaException("Default value is not supported for type: type=" + type.toString());
        }
    }

    /**
     * Build schema descriptor by SchemaTable.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @param tblCfg SchemaTable.
     * @return SchemaDescriptor.
     */
    public static SchemaDescriptor convert(UUID tblId, int schemaVer, SchemaTable tblCfg) {
        List<org.apache.ignite.schema.Column> keyColsCfg = new ArrayList<>(tblCfg.keyColumns());

        Column[] keyCols = new Column[keyColsCfg.size()];

        for (int i = 0; i < keyCols.length; i++)
            keyCols[i] = convert(keyColsCfg.get(i));

        String[] affCols = tblCfg.affinityColumns().stream().map(org.apache.ignite.schema.Column::name)
            .toArray(String[]::new);

        List<org.apache.ignite.schema.Column> valColsCfg = new ArrayList<>(tblCfg.valueColumns());

        Column[] valCols = new Column[valColsCfg.size()];

        for (int i = 0; i < valCols.length; i++)
            valCols[i] = convert(valColsCfg.get(i));

        return new SchemaDescriptor(tblId, schemaVer, keyCols, affCols, valCols);
    }

    /**
     * Constant value supplier.
     */
    private static class ConstantSupplier implements Supplier<Object>, Serializable {
        /** Value. */
        private final Serializable val;

        /**
         * @param val Value.
         */
        ConstantSupplier(Serializable val) {
            this.val = val;
        }

        /** {@inheritDoc */
        @Override public Object get() {
            return val;
        }
    }
}
