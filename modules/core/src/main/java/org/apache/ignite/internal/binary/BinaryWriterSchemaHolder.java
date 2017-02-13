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

import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

import static org.apache.ignite.internal.binary.BinaryUtils.FIELD_ID_LEN;
import static org.apache.ignite.internal.binary.BinaryUtils.MAX_OFFSET_1;
import static org.apache.ignite.internal.binary.BinaryUtils.MAX_OFFSET_2;
import static org.apache.ignite.internal.binary.BinaryUtils.OFFSET_1;
import static org.apache.ignite.internal.binary.BinaryUtils.OFFSET_2;
import static org.apache.ignite.internal.binary.BinaryUtils.OFFSET_4;

/**
 * Binary writer schema holder.
 */
public class BinaryWriterSchemaHolder {

    /** Grow step. */
    private static final int GROW_STEP = 64;

    /** Data. */
    private int[] data = new int[GROW_STEP];

    /** Index. */
    private int idx;

    /**
     * Push another frame.
     *
     * @param id Field ID.
     * @param off Field offset.
     */
    public void push(int id, int off) {
        if (idx == data.length) {
            int[] data0 = new int[data.length + GROW_STEP];

            System.arraycopy(data, 0, data0, 0, data.length);

            data = data0;
        }

        data[idx] = id;
        data[idx + 1] = off;

        idx += 2;
    }

    /**
     * Build the schema.
     *
     * @param builder Builder.
     * @param fieldCnt Fields count.
     */
    public void build(BinarySchema.Builder builder, int fieldCnt) {
        for (int curIdx = idx - fieldCnt * 2; curIdx < idx; curIdx += 2)
            builder.addField(data[curIdx]);
    }

    /**
     * Write collected frames and pop them.
     *
     * @param out Output stream.
     * @param fieldCnt Count.
     * @param compactFooter Whether footer should be written in compact form.
     * @return Amount of bytes dedicated to each field offset. Could be 1, 2 or 4.
     */
    public int write(BinaryOutputStream out, int fieldCnt, boolean compactFooter) {
        int startIdx = idx - fieldCnt * 2;
        assert startIdx >= 0;

        int lastOffset = data[idx - 1];

        int res;

        if (compactFooter) {
            if (lastOffset < MAX_OFFSET_1) {
                out.unsafeEnsure(fieldCnt);
                for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2)
                    out.unsafeWriteByte((byte)data[curIdx]);

                res = OFFSET_1;
            }
            else if (lastOffset < MAX_OFFSET_2) {
                out.unsafeEnsure(fieldCnt * OFFSET_2);
                for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2)
                    out.unsafeWriteShort((short) data[curIdx]);

                res = OFFSET_2;
            }
            else {
                out.unsafeEnsure(fieldCnt * OFFSET_4);
                for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2)
                    out.unsafeWriteInt(data[curIdx]);

                res = OFFSET_4;
            }
        }
        else {
            if (lastOffset < MAX_OFFSET_1) {
                out.unsafeEnsure(fieldCnt * (OFFSET_1 + FIELD_ID_LEN));
                for (int curIdx = startIdx; curIdx < idx;) {
                    out.unsafeWriteInt(data[curIdx++]);
                    out.unsafeWriteByte((byte) data[curIdx++]);
                }

                res = OFFSET_1;
            }
            else if (lastOffset < MAX_OFFSET_2) {
                out.unsafeEnsure(fieldCnt * (OFFSET_2 + FIELD_ID_LEN));
                for (int curIdx = startIdx; curIdx < idx;) {
                    out.unsafeWriteInt(data[curIdx++]);
                    out.unsafeWriteShort((short) data[curIdx++]);
                }

                res = OFFSET_2;
            }
            else {
                out.unsafeEnsure(fieldCnt * (OFFSET_4 + FIELD_ID_LEN));
                for (int curIdx = startIdx; curIdx < idx;) {
                    out.unsafeWriteInt(data[curIdx++]);
                    out.unsafeWriteInt(data[curIdx++]);
                }

                res = OFFSET_4;
            }
        }

        return res;
    }

    /**
     * Pop current object's frame.
     */
    public void pop(int fieldCnt) {
        idx = idx - fieldCnt * 2;
    }
}
