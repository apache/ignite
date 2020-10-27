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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.h2.table.Column;
import org.h2.value.CompareMode;
import org.h2.value.Value;
import org.h2.value.ValueInt;

/**
 * Inline index column implementation for inlining hash of Java objects.
 */
public class ObjectHashInlineIndexColumn extends AbstractInlineIndexColumn {
    /**
     * @param col Column.
     */
    public ObjectHashInlineIndexColumn(Column col) {
        super(col, Value.JAVA_OBJECT, (short)4);
    }

    /** {@inheritDoc} */
    @Override protected int compare0(long pageAddr, int off, Value v, int type) {
        // Exact type matching is required here to avoid cases when SQL types compare with JAVA_OBJECT.
        // It's may be cause of unexpected behavior.
        if (type() != type || type != v.getType())
            return COMPARE_UNSUPPORTED;

        int val1 = PageUtils.getInt(pageAddr, off + 1);
        int val2 = v.getObject().hashCode();

        int res = Integer.signum(Integer.compare(val1, val2));

        return res == 0 ? CANT_BE_COMPARE : res;
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Value val, int maxSize) {
        assert type() == val.getType();

        PageUtils.putByte(pageAddr, off, (byte)val.getType());
        PageUtils.putInt(pageAddr, off + 1, val.getObject().hashCode());

        return size() + 1;
    }

    /** {@inheritDoc} */
    @Override protected Value get0(long pageAddr, int off) {
        int hashCode = PageUtils.getInt(pageAddr, off + 1);
        return new ValueObjectHashCode(hashCode); //TODO Is this ok?
    }

    /**
     * Used for test purpose only.
     *
     * @param pageAddr Address of the page.
     * @param off Offset of the record.
     * @return Actual hash value that has been inlined.
     */
    ValueInt inlinedValue(long pageAddr, int off) {
        return ValueInt.get(PageUtils.getInt(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSizeOf0(Value val) {
        assert val.getType() == type();

        return size() + 1;
    }

    /**
     * Value for object with hashcode.
     */
    private static class ValueObjectHashCode extends Value {
        /**
         * The precision in digits.
         */
        public static final int PRECISION = 10;

        /**
         * The maximum display size of an int.
         * Example: -2147483648
         */
        public static final int DISPLAY_SIZE = 11;

        /**
         * Hashcode of object.
         */
        private final int value;

        public ValueObjectHashCode(int value) {
            this.value = value;
        }

        /** {@inheritDoc} */
        @Override public String getSQL() {
            return getString();
        }

        /** {@inheritDoc} */
        @Override public int getType() {
            return Value.JAVA_OBJECT;
        }

        /** {@inheritDoc} */
        @Override public int getInt() {
            return value;
        }

        /** {@inheritDoc} */
        @Override public long getLong() {
            return value;
        }

        /** {@inheritDoc} */
        @Override protected int compareSecure(Value o, CompareMode mode) {
            ValueObjectHashCode v = (ValueObjectHashCode) o;
            return Integer.compare(value, v.value);
        }

        /** {@inheritDoc} */
        @Override public String getString() {
            return String.valueOf(value);
        }

        /** {@inheritDoc} */
        @Override public long getPrecision() {
            return PRECISION;
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return value;
        }

        /** {@inheritDoc} */
        @Override public Object getObject() {
            return value;
        }

        /** {@inheritDoc} */
        @Override public void set(PreparedStatement prep, int parameterIndex)
                throws SQLException {
            prep.setInt(parameterIndex, value);
        }

        /** {@inheritDoc} */
        @Override public int getDisplaySize() {
            return DISPLAY_SIZE;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object other) {
            return other instanceof ValueObjectHashCode && value == ((ValueObjectHashCode) other).value;
        }
    }
}
