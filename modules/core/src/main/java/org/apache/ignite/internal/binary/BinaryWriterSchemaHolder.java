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

/**
 * Binary writer schema holder.
 */
public class BinaryWriterSchemaHolder {
    /** Maximum offset which fits in 1 byte. */
    private static final int MAX_OFFSET_1 = 1 << 8;

    /** Maximum offset which fits in 2 bytes. */
    private static final int MAX_OFFSET_2 = 1 << 16;

    /** Grow step. */
    private static final int GROW_STEP = 64;

    /** Data. */
    private int[] data = new int[GROW_STEP];

    /** Data. */
    private boolean[] isFieldNull = new boolean[GROW_STEP];

    /** Index. */
    private int idx;

    /**
     * Push another frame.
     *
     * @param id Field ID.
     * @param off Field offset.
     */
    public void push(int id, int off, boolean isNull) {

        if (idx == isFieldNull.length) {
            boolean[] isFieldNull0 = new boolean[isFieldNull.length + GROW_STEP];

            System.arraycopy(isFieldNull, 0, isFieldNull0, 0, isFieldNull.length);

            isFieldNull = isFieldNull0;
        }
        if (idx == data.length) {
            int[] data0 = new int[data.length + GROW_STEP];

            System.arraycopy(data, 0, data0, 0, data.length);

            data = data0;
        }

        data[idx] = id;
        data[idx +  1] = off;

        this.isFieldNull[idx/2] = isNull;

        idx += 2;

    }

    /**
     * Push another frame.
     *
     * @param id Field ID.
     * @param off Field offset.
     */
    public void push(int id, int off) {
        push(id, off, false);

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
     * @param compactNull Whether null are compacted
     * @return Amount of bytes dedicated to each field offset. Could be 1, 2 or 4.
     */
    public int write(BinaryOutputStream out, int fieldCnt, boolean compactFooter, boolean compactNull) {
        int startIdx = idx - fieldCnt * 2;
        assert startIdx >= 0;

        // Ensure there are at least 8 bytes for each field to allow for unsafe writes.
        out.unsafeEnsure(fieldCnt << 3);

        int lastOffset = data[idx - 1];

        int res;

        if (compactFooter) {
            if (compactNull && fieldCnt > 0) {
               byte[] nullMask = BinaryClassDescriptor.createNullMask(fieldCnt);

               // If nulls are compacted then offset are only written if the current offset is != 0,
               // meaning this is not a null field.
               // Warning: index does not start at zero as the frames as nested
               for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2) {
                       int fieldIndex = ((curIdx - startIdx) / 2) ;
                       if (!isFieldNull[curIdx / 2]) {
                           // If the isFieldNull[curIdx / 2] != 0 then the field is not null
                           // Compute which mask byte and bit to update
                           byte maskOffsetByte = (byte) (fieldIndex / 8);
                           byte maskOffsetBit = (byte) ((fieldIndex) % 8);
                           nullMask[maskOffsetByte] = (byte) (nullMask[maskOffsetByte] | 1 << (maskOffsetBit));

                           writeOffset(out, lastOffset, curIdx, true);
                       }// If the offset is 0 then the field is null and thus there is nothing to do
               }
               // If compact null is enable, the null mask is appended at the end of the footer
               // Please note that when several bytes are required (fieldCnt>8) the first byte is written first
               for (int i = 0; i < nullMask.length; i++) {
                       out.unsafeWriteByte(nullMask[i]);
               }
            } else {
               //No null compaction, default to write all null indexes
               for (int curIdx = startIdx + 1; curIdx < idx; curIdx += 2) {
                   writeOffset(out, lastOffset, curIdx, true);
               }
           }
       } else {
               for (int curIdx = startIdx; curIdx < idx; curIdx += 2)
                       writeOffset(out, lastOffset, curIdx, false);
       }

       if (lastOffset < MAX_OFFSET_1) {
               res = BinaryUtils.OFFSET_1;
       } else if (lastOffset < MAX_OFFSET_2) {
               res = BinaryUtils.OFFSET_2;
       } else {
               res = BinaryUtils.OFFSET_4;
       }
       return res;
    }

    /**
     * @param out
     * @param lastOffset
     * @param curIdx
     * @param isCompactFooterEnabled
     */
    private void writeOffset(BinaryOutputStream out, int lastOffset, int curIdx, boolean isCompactFooterEnabled) {
       if (!isCompactFooterEnabled)
               out.unsafeWriteInt(data[curIdx++]);

       if (lastOffset < MAX_OFFSET_1) {
               out.unsafeWriteByte((byte) data[curIdx]);
       } else if (lastOffset < MAX_OFFSET_2) {
               out.unsafeWriteShort((short) data[curIdx]);
       } else {
               out.unsafeWriteInt(data[curIdx]);
       }
    }

    /**
     * Pop current object's frame.
     */
    public void pop(int fieldCnt) {
        idx = idx - fieldCnt * 2;
    }
}
