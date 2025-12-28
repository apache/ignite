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

package org.apache.ignite.internal.binary;

import java.lang.reflect.Field;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Field accessor to speedup access.
 */
class BinaryFieldAccessor {
    /** Field ID. */
    protected final int id;

    /** Field name */
    protected final String name;

    /** Mode. */
    protected final BinaryWriteMode mode;

    /** Offset. Used for primitive fields, only. */
    protected final long offset;

    /** Target field. */
    protected final Field field;

    /** Dynamic accessor flag. */
    protected final boolean dynamic;

    /**
     * Create accessor for the field.
     *
     * @param field Field.
     * @param id FIeld ID.
     * @return Accessor.
     */
    public static BinaryFieldAccessor create(Field field, int id) {
        BinaryWriteMode mode = BinaryUtils.mode(field.getType());

        switch (mode) {
            case P_BYTE:
                return new BinaryFieldAccessor(field, id, mode, GridUnsafe.objectFieldOffset(field), false);

            case BYTE:
            case BOOLEAN:
            case SHORT:
            case CHAR:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case STRING:
            case UUID:
            case DATE:
            case TIMESTAMP:
            case TIME:
            case BYTE_ARR:
            case SHORT_ARR:
            case INT_ARR:
            case LONG_ARR:
            case FLOAT_ARR:
            case DOUBLE_ARR:
            case CHAR_ARR:
            case BOOLEAN_ARR:
            case DECIMAL_ARR:
            case STRING_ARR:
            case UUID_ARR:
            case DATE_ARR:
            case TIMESTAMP_ARR:
            case TIME_ARR:
            case ENUM_ARR:
            case OBJECT_ARR:
            case BINARY_OBJ:
            case BINARY:
                return new BinaryFieldAccessor(field, id, mode, -1L, false);

            default:
                return new BinaryFieldAccessor(field, id, mode, -1L, !CommonUtils.isFinal(field.getType()));
        }
    }

    /**
     * Protected constructor.
     *
     * @param id Field ID.
     * @param mode Mode;
     */
    protected BinaryFieldAccessor(Field field, int id, BinaryWriteMode mode, long offset, boolean dynamic) {
        assert field != null;
        assert id != 0;
        assert mode != null;

        this.name = field.getName();
        this.id = id;
        this.mode = mode;
        this.offset = offset;
        this.field = field;
        this.dynamic = dynamic;
    }

    /**
     * Get mode.
     *
     * @return Mode.
     */
    public BinaryWriteMode mode() {
        return mode;
    }
}
