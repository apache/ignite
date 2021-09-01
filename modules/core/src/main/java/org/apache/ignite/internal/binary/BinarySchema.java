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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Schema describing binary object content. We rely on the following assumptions:
 * - When amount of fields in the object is low, it is better to inline these values into int fields thus allowing
 * for quick comparisons performed within already fetched L1 cache line.
 * - When there are more fields, we store them inside a hash map.
 */
public class BinarySchema implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Order returned if field is not found. */
    public static final int ORDER_NOT_FOUND = -1;

    /** Minimum sensible size. */
    private static final int MAP_MIN_SIZE = 32;

    /** Empty cell. */
    private static final int MAP_EMPTY = 0;

    /** Schema ID. */
    private int schemaId;

    /** IDs depending on order. */
    private int[] ids;

    /** Interned names of associated fields. */
    private String[] names;

    /** ID-to-order data. */
    private int[] idToOrderData;

    /** ID-to-order mask. */
    private int idToOrderMask;

    /** ID 1. */
    private int id0;

    /** ID 2. */
    private int id1;

    /** ID 3. */
    private int id2;

    /** ID 4. */
    private int id3;

    /**
     * {@link Externalizable} support.
     */
    public BinarySchema() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schemaId Schema ID.
     * @param fieldIds Field IDs.
     */
    public BinarySchema(int schemaId, List<Integer> fieldIds) {
        assert fieldIds != null;

        this.schemaId = schemaId;

        initialize(fieldIds);
    }

    /**
     * @return Schema ID.
     */
    public int schemaId() {
        return schemaId;
    }

    /**
     * Try speculatively confirming order for the given field name.
     *
     * @param expOrder Expected order.
     * @param expName Expected name.
     * @return Field ID.
     */
    @SuppressWarnings("StringEquality")
    public Confirmation confirmOrder(int expOrder, String expName) {
        assert expName != null;

        if (expOrder < names.length) {
            String name = names[expOrder];

            // Note that we use only reference equality assuming that field names are interned literals.
            if (name == expName)
                return Confirmation.CONFIRMED;

            if (name == null)
                return Confirmation.CLARIFY;
        }

        return Confirmation.REJECTED;
    }

    /**
     * Add field name.
     *
     * @param order Order.
     * @param name Name.
     */
    public void clarifyFieldName(int order, String name) {
        assert name != null;
        assert order < names.length;

        names[order] = name.intern();
    }

    /**
     * Get field ID by order in footer.
     *
     * @param order Order.
     * @return Field ID.
     */
    public int fieldId(int order) {
        return order < ids.length ? ids[order] : 0;
    }

    /**
     * Get field order in footer by field ID.
     *
     * @param id Field ID.
     * @return Offset or {@code 0} if there is no such field.
     */
    public int order(int id) {
        if (idToOrderData == null) {
            if (id == id0)
                return 0;

            if (id == id1)
                return 1;

            if (id == id2)
                return 2;

            if (id == id3)
                return 3;

            return ORDER_NOT_FOUND;
        }
        else {
            int idx = (id & idToOrderMask) << 1;

            int curId = idToOrderData[idx];

            if (id == curId) // Hit!
                return idToOrderData[idx + 1];
            else if (curId == MAP_EMPTY) // No such ID!
                return ORDER_NOT_FOUND;
            else {
                // Unlikely collision scenario.
                for (int i = 2; i < idToOrderData.length; i += 2) {
                    int newIdx = (idx + i) % idToOrderData.length;

                    assert newIdx < idToOrderData.length - 1;

                    curId = idToOrderData[newIdx];

                    if (id == curId)
                        return idToOrderData[newIdx + 1];
                    else if (curId == MAP_EMPTY)
                        return ORDER_NOT_FOUND;
                }

                return ORDER_NOT_FOUND;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return schemaId;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        return o != null && o instanceof BinarySchema && schemaId == ((BinarySchema)o).schemaId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinarySchema.class, this,
            "ids", Arrays.toString(ids),
            "names", Arrays.toString(names),
            "idToOrderData", Arrays.toString(idToOrderData));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        writeTo(out);
    }

    /**
     * The object implements the writeTo method to save its contents
     * by calling the methods of DataOutput for its primitive values and strings or
     * calling the writeTo method for other objects.
     *
     * @param out the stream to write the object to.
     * @throws IOException Includes any I/O exceptions that may occur.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeInt(schemaId);

        out.writeInt(ids.length);

        for (Integer id : ids)
            out.writeInt(id);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFrom(in);
    }

    /**
     * The object implements the readFrom method to restore its
     * contents by calling the methods of DataInput for primitive
     * types and strings or calling readFrom for other objects.  The
     * readFrom method must read the values in the same sequence
     * and with the same types as were written by writeTo.
     *
     * @param in the stream to read data from in order to restore the object
     * @throws IOException if I/O errors occur
     */
    public void readFrom(DataInput in) throws IOException {
        schemaId = in.readInt();

        int idsCnt = in.readInt();

        List<Integer> fieldIds = new ArrayList<>(idsCnt);

        for (int i = 0; i < idsCnt; i++)
            fieldIds.add(in.readInt());

        initialize(fieldIds);
    }

    /**
     * Gets field ids array.
     *
     * @return Field ids.
     */
    public int[] fieldIds() {
        return ids;
    }

    /**
     * Parse values.
     *
     * @param vals Values.
     * @param size Proposed result size.
     * @return Parse result.
     */
    private static ParseResult parse(int[] vals, int size) {
        int mask = maskForPowerOfTwo(size);

        int totalSize = size * 2;

        int[] data = new int[totalSize];
        int collisions = 0;

        for (int order = 0; order < vals.length; order++) {
            int id = vals[order];

            assert id != 0;

            int idIdx = (id & mask) << 1;

            if (data[idIdx] == 0) {
                // Found empty slot.
                data[idIdx] = id;
                data[idIdx + 1] = order;
            }
            else {
                // Collision!
                collisions++;

                boolean placeFound = false;

                for (int i = 2; i < totalSize; i += 2) {
                    int newIdIdx = (idIdx + i) % totalSize;

                    if (data[newIdIdx] == 0) {
                        data[newIdIdx] = id;
                        data[newIdIdx + 1] = order;

                        placeFound = true;

                        break;
                    }
                }

                assert placeFound : "Should always have a place for entry!";
            }
        }

        return new ParseResult(data, collisions);
    }

    /**
     * Get next power of two which greater or equal to the given number.
     * This implementation is not meant to be very efficient, so it is expected to be used relatively rare.
     *
     * @param val Number
     * @return Nearest pow2.
     */
    private static int nextPowerOfTwo(int val) {
        int res = 1;

        while (res < val)
            res = res << 1;

        if (res < 0)
            throw new IllegalArgumentException("Value is too big to find positive pow2: " + val);

        return res;
    }

    /**
     * Calculate mask for the given value which is a power of two.
     *
     * @param val Value.
     * @return Mask.
     */
    private static int maskForPowerOfTwo(int val) {
        int mask = 0;
        int comparand = 1;

        while (comparand < val) {
            mask |= comparand;

            comparand <<= 1;
        }

        return mask;
    }

    /**
     * Initialization routine.
     *
     * @param fieldIds Field IDs.
     */
    private void initialize(List<Integer> fieldIds) {
        ids = new int[fieldIds.size()];

        for (int i = 0; i < fieldIds.size(); i++)
            ids[i] = fieldIds.get(i);

        names = new String[fieldIds.size()];

        if (fieldIds.size() <= 4) {
            Iterator<Integer> iter = fieldIds.iterator();

            id0 = iter.hasNext() ? iter.next() : 0;
            id1 = iter.hasNext() ? iter.next() : 0;
            id2 = iter.hasNext() ? iter.next() : 0;
            id3 = iter.hasNext() ? iter.next() : 0;
        }
        else {
            id0 = id1 = id2 = id3 = 0;

            initializeMap(ids);
        }
    }

    /**
     * Initialize the map.
     *
     * @param vals Values.
     */
    private void initializeMap(int[] vals) {
        int size = Math.max(nextPowerOfTwo(vals.length) << 2, MAP_MIN_SIZE);

        assert size > 0;

        ParseResult finalRes;

        ParseResult res1 = parse(vals, size);

        if (res1.collisions == 0)
            finalRes = res1;
        else {
            ParseResult res2 = parse(vals, size * 2);

            // Failed to decrease aom
            if (res2.collisions == 0)
                finalRes = res2;
            else
                finalRes = parse(vals, size * 4);
        }

        idToOrderData = finalRes.data;
        idToOrderMask = maskForPowerOfTwo(idToOrderData.length / 2);
    }

    /**
     * Schema builder.
     */
    public static class Builder {
        /** Schema ID. */
        private int schemaId = BinaryUtils.schemaInitialId();

        /** Fields. */
        private final ArrayList<Integer> fields = new ArrayList<>();

        /**
         * Create new schema builder.
         *
         * @return Schema builder.
         */
        public static Builder newBuilder() {
            return new Builder();
        }

        /**
         * Private constructor.
         */
        private Builder() {
            // No-op.
        }

        /**
         * Add field.
         *
         * @param fieldId Field ID.
         */
        public void addField(int fieldId) {
            fields.add(fieldId);

            schemaId = BinaryUtils.updateSchemaId(schemaId, fieldId);
        }

        /**
         * Build schema.
         *
         * @return Schema.
         */
        public BinarySchema build() {
            return new BinarySchema(schemaId, fields);
        }
    }

    /**
     * Order confirmation result.
     */
    public enum Confirmation {
        /** Confirmed. */
        CONFIRMED,

        /** Denied. */
        REJECTED,

        /** Field name clarification is needed. */
        CLARIFY
    }

    /**
     * Result of map parsing.
     */
    private static class ParseResult {
        /** Data. */
        private int[] data;

        /** Collisions. */
        private int collisions;

        /**
         * Constructor.
         *
         * @param data Data.
         * @param collisions Collisions.
         */
        private ParseResult(int[] data, int collisions) {
            this.data = data;
            this.collisions = collisions;
        }
    }
}
