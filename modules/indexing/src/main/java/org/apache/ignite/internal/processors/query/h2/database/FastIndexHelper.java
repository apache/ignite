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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.h2.table.IndexColumn;
import org.h2.value.Value;
import org.h2.value.ValueBoolean;
import org.h2.value.ValueByte;
import org.h2.value.ValueInt;
import org.h2.value.ValueLong;

/**
 * Helper class for in-page indexes.
 */
public class FastIndexHelper {
    /** */
    public static final List<Integer> AVAILABLE_TYPES = Arrays.asList(Value.BOOLEAN, Value.BYTE, Value.SHORT, Value.INT, Value.LONG);

    /** */
    private final int type;
    /** */
    private final int colIdx;
    /** */
    private final int sortType;

    /**
     * @param type Index type (see {@link Value}).
     * @param colIdx Index column index.
     * @param sortType Column sort type (see {@link IndexColumn#sortType}).
     */
    public FastIndexHelper(int type, int colIdx, int sortType) {
        this.type = type;
        this.colIdx = colIdx;
        this.sortType = sortType;
    }

    /** */
    public int type() {
        return type;
    }

    /** */
    public int columnIdx() {
        return colIdx;
    }

    /** */
    public int sortType() {
        return sortType;
    }

    /** */
    public int size() {
        switch (type) {
            case Value.BOOLEAN:
            case Value.BYTE:
                return 1;

            case Value.INT:
                return 4;

            case Value.SHORT:
                return 2;

            case Value.LONG:
                return 8;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /** */
    public Value get(long pageAddr, int off) {
        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get(PageUtils.getByte(pageAddr, off) != 0);

            case Value.BYTE:
                return ValueByte.get(PageUtils.getByte(pageAddr, off));

            case Value.SHORT:
                return ValueInt.get(PageUtils.getShort(pageAddr, off));

            case Value.INT:
                return ValueInt.get(PageUtils.getInt(pageAddr, off));

            case Value.LONG:
                return ValueLong.get(PageUtils.getLong(pageAddr, off));

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /** */
    public Value get(ByteBuffer buf, int off) {
        switch (type) {
            case Value.BOOLEAN:
                return ValueBoolean.get(buf.get(off) != 0);

            case Value.BYTE:
                return ValueByte.get(buf.get(off));

            case Value.SHORT:
                return ValueInt.get(buf.getShort(off));

            case Value.INT:
                return ValueInt.get(buf.getInt(off));

            case Value.LONG:
                return ValueLong.get(buf.getLong(off));

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /** */
    public void put(long pageAddr, int off, Value val) {
        switch (type) {
            case Value.BOOLEAN:
                PageUtils.putByte(pageAddr, off, (byte)(val.getBoolean() ? 1 : 0));
                break;

            case Value.BYTE:
                PageUtils.putByte(pageAddr, off, val.getByte());
                break;

            case Value.SHORT:
                PageUtils.putShort(pageAddr, off, val.getShort());
                break;

            case Value.INT:
                PageUtils.putInt(pageAddr, off, val.getInt());
                break;

            case Value.LONG:
                PageUtils.putLong(pageAddr, off, val.getLong());
                break;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }

    /** */
    public void put(ByteBuffer buf, int off, Value val) {
        switch (type) {
            case Value.BOOLEAN:
                buf.put(off, (byte)(val.getBoolean() ? 1 : 0));
                break;

            case Value.BYTE:
                buf.put(off, val.getByte());
                break;

            case Value.SHORT:
                buf.putShort(off, val.getShort());
                break;

            case Value.INT:
                buf.putInt(off, val.getInt());
                break;

            case Value.LONG:
                buf.putLong(off, val.getLong());
                break;

            default:
                throw new UnsupportedOperationException("no get operation for fast index type " + type);
        }
    }
}
