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

package org.apache.ignite.internal.processors.query.h2.database.inlinecolumn;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.processors.query.h2.database.InlineIndexColumn;
import org.apache.ignite.internal.util.lang.GridTuple;
import org.h2.table.Column;
import org.h2.value.CompareMode;
import org.h2.value.Value;

/**
 * Factory to create inline column wrapper for given column.
 */
public class InlineIndexColumnFactory {
    /** PageContext for use in IO's */
    private static final ThreadLocal<GridTuple<List<InlineIndexColumn>>> CUR_HELPER =
        ThreadLocal.withInitial(GridTuple::new);

    /** */
    private static final Set<Integer> AVAILABLE_TYPES = new HashSet<>(Arrays.asList(
        Value.BOOLEAN,
        Value.BYTE,
        Value.BYTES,
        Value.DATE,
        Value.DOUBLE,
        Value.FLOAT,
        Value.INT,
        Value.LONG,
        Value.JAVA_OBJECT,
        Value.SHORT,
        Value.STRING,
        Value.STRING_FIXED,
        Value.STRING_IGNORECASE,
        Value.TIME,
        Value.TIMESTAMP,
        Value.UUID
    ));

    /** */
    private final Map<String, InlineIndexColumn> helpers = new HashMap<>();

    /**
     * Creates inline column wrapper for given indexed column.
     *
     * @param col Column.
     */
    private static InlineIndexColumn createHelper(CompareMode mode, Column col) {
        boolean useOptimizedStrComp = CompareMode.OFF.equals(mode.getName());

        switch (col.getType()) {
            case Value.BOOLEAN:
                return new BooleanInlineIndexColumn(col);

            case Value.BYTE:
                return new ByteInlineIndexColumn(col);

            case Value.SHORT:
                return new ShortInlineIndexColumn(col);

            case Value.INT:
                return new IntegerInlineIndexColumn(col);

            case Value.LONG:
                return new LongInlineIndexColumn(col);

            case Value.FLOAT:
                return new FloatInlineIndexColumn(col);

            case Value.DOUBLE:
                return new DoubleInlineIndexColumn(col);

            case Value.DATE:
                return new DateInlineIndexColumn(col);

            case Value.TIME:
                return new TimeInlineIndexColumn(col);

            case Value.TIMESTAMP:
                return new TimestampInlineIndexColumn(col);

            case Value.UUID:
                return new UuidInlineIndexColumn(col);

            case Value.STRING:
                return new StringInlineIndexColumn(col, useOptimizedStrComp);

            case Value.STRING_FIXED:
                return new FixedStringInlineIndexColumn(col, useOptimizedStrComp);

            case Value.STRING_IGNORECASE:
                return new StringIgnoreCaseInlineIndexColumn(col, useOptimizedStrComp);

            case Value.BYTES:
                return new BytesInlineIndexColumn(col, mode.isBinaryUnsigned());

            case Value.JAVA_OBJECT:
                return new ObjectHashInlineIndexColumn(col);
        }

        throw new IllegalStateException("Unknown value type=" + col.getType());
    }

    /** */
    private final CompareMode mode;

    /**
     * @param col Column.
     * @param inlineObjHash Inline object hash.
     */
    public InlineIndexColumn createInlineHelper(Column col, boolean inlineObjHash) {
        int type = col.getType();

        if (type == Value.JAVA_OBJECT && !inlineObjHash)
            // fallback to old inlining logic
            return new ObjectBytesInlineIndexColumn(col, mode.isBinaryUnsigned());

        return helpers.computeIfAbsent(col.getName(), name -> createHelper(mode, col));
    }

    /**
     * @param type Type.
     */
    public static boolean typeSupported(Integer type) {
        return AVAILABLE_TYPES.contains(type);
    }

    /**
     * @param mode Compare mode.
     */
    public InlineIndexColumnFactory(CompareMode mode) {
        this.mode = mode;
    }

    /**
     * @return Page context for current thread.
     */
    public static List<InlineIndexColumn> getCurrentInlineIndexes() {
        return CUR_HELPER.get().get();
    }

    /**
     * Sets page context for current thread.
     */
    public static void setCurrentInlineIndexes(List<InlineIndexColumn> inlineIdxs) {
        CUR_HELPER.get().set(inlineIdxs);
    }

    /**
     * Clears current context.
     */
    public static void clearCurrentInlineIndexes() {
        CUR_HELPER.get().set(null);
    }

    /**
     * @param typeCode Type code.
     * @return Name.
     */
    public static String nameTypeByCode(int typeCode) {
        switch (typeCode) {
            case Value.UNKNOWN:
                return "UNKNOWN";
            case Value.NULL:
                return "NULL";
            case Value.BOOLEAN:
                return "BOOLEAN";
            case Value.BYTE:
                return "BYTE";
            case Value.SHORT:
                return "SHORT";
            case Value.INT:
                return "INT";
            case Value.LONG:
                return "LONG";
            case Value.DECIMAL:
                return "DECIMAL";
            case Value.DOUBLE:
                return "DOUBLE";
            case Value.FLOAT:
                return "FLOAT";
            case Value.TIME:
                return "TIME";
            case Value.DATE:
                return "DATE";
            case Value.TIMESTAMP:
                return "TIMESTAMP";
            case Value.BYTES:
                return "BYTES";
            case Value.STRING:
                return "STRING";
            case Value.STRING_IGNORECASE:
                return "STRING_IGNORECASE";
            case Value.BLOB:
                return "BLOB";
            case Value.CLOB:
                return "CLOB";
            case Value.ARRAY:
                return "ARRAY";
            case Value.RESULT_SET:
                return "RESULT_SET";
            case Value.JAVA_OBJECT:
                return "JAVA_OBJECT";
            case Value.UUID:
                return "UUID";
            case Value.STRING_FIXED:
                return "STRING_FIXED";
            case Value.GEOMETRY:
                return "GEOMETRY";
            case Value.TIMESTAMP_TZ:
                return "TIMESTAMP_TZ";
            case Value.ENUM:
                return "ENUM";
            default:
                return "UNKNOWN type " + typeCode;
        }
    }
}
