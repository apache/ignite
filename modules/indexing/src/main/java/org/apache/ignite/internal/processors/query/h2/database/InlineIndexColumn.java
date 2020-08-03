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

package org.apache.ignite.internal.processors.query.h2.database;

import java.util.Comparator;
import org.h2.value.Value;

/**
 * Basic interface for inlined index columns.
 */
public interface InlineIndexColumn {
    /**
     * Returns type of inlined column.
     *
     * @return Integer code of the column's value type.
     * @see Value
     */
    public int type();

    /**
     * Size required to inline value of type {@link #type()}.
     *
     * @return Amount of bytes required for fixed-size types or {@code -1}
     * for types with variable length.
     */
    public short size();

    /**
     * Actual size of inline value. It returns size() + 1 for values with
     * fixed size and amount of written bytes for values with variable length.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @return Returns actual size of inlined value.
     */
    public int fullSize(long pageAddr, int off);

    /**
     * Compares inlined and given value.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @param maxSize Max size.
     * @param v Value that should be compare.
     * @param comp Comparator.
     *
     * @return -1, 0 or 1 if inlined value less, equal or greater
     * than given respectively, or -2 if inlined part is not enough to compare.
     */
    public int compare(long pageAddr, int off, int maxSize, Value v, Comparator<Value> comp);

    /**
     * Calculate size required to inline given value.
     *
     * @param val Value to calculate inline size.
     *
     * @return Calculated inline size.
     */
    public int inlineSizeOf(Value val);

    /**
     * Puts given value into inline index tree.
     *
     * @param pageAddr Page address.
     * @param off Offset.
     * @param val Value.
     * @param maxSize Max size.
     *
     * @return Amount of bytes actually stored.
     */
    public int put(long pageAddr, int off, Value val, int maxSize);

    /**
     * Returns name of the inlined column.
     *
     * @return Name of the column.
     */
    public String columnName();

    /**
     * Returns index of the inlined column.
     *
     * @return Index of the column.
     */
    public int columnIndex();
}
