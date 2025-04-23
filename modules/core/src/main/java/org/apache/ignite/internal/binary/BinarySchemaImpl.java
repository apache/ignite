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

import static org.apache.ignite.internal.binary.BinaryUtils.MAP_EMPTY;
import static org.apache.ignite.internal.binary.BinaryUtils.MAP_MIN_SIZE;
import static org.apache.ignite.internal.binary.BinaryUtils.ORDER_NOT_FOUND;

/**
 * Schema describing binary object content. We rely on the following assumptions:
 * - When amount of fields in the object is low, it is better to inline these values into int fields thus allowing
 * for quick comparisons performed within already fetched L1 cache line.
 * - When there are more fields, we store them inside a hash map.
 */
class BinarySchemaImpl implements BinarySchema, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

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
    public BinarySchemaImpl() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param schemaId Schema ID.
     * @param fieldIds Field IDs.
     */
    public BinarySchemaImpl(int schemaId, List<Integer> fieldIds) {
        assert fieldIds != null;

        this.schemaId = schemaId;

        initialize(fieldIds);
    }

    /** {@inheritDoc} */
    @Override public int schemaId() {
        return schemaId;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("StringEquality")
    @Override public Confirmation confirmOrder(int expOrder, String expName) {
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

    /** {@inheritDoc} */
    @Override public void clarifyFieldName(int order, String name) {
        assert name != null;
        assert order < names.length;

        names[order] = name.intern();
    }

    /** {@inheritDoc} */
    @Override public int fieldId(int order) {
        return order < ids.length ? ids[order] : 0;
    }

    /** {@inheritDoc} */
    @Override public int order(int id) {
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
        return o != null && o instanceof BinarySchemaImpl && schemaId == ((BinarySchemaImpl)o).schemaId;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinarySchemaImpl.class, this,
            "ids", Arrays.toString(ids),
            "names", Arrays.toString(names),
            "idToOrderData", Arrays.toString(idToOrderData));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        writeTo(out);
    }

    /** {@inheritDoc} */
    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(schemaId);

        out.writeInt(ids.length);

        for (Integer id : ids)
            out.writeInt(id);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        readFrom(in);
    }

    /** {@inheritDoc} */
    @Override public void readFrom(DataInput in) throws IOException {
        schemaId = in.readInt();

        int idsCnt = in.readInt();

        List<Integer> fieldIds = new ArrayList<>(idsCnt);

        for (int i = 0; i < idsCnt; i++)
            fieldIds.add(in.readInt());

        initialize(fieldIds);
    }

    /** {@inheritDoc} */
    @Override public int[] fieldIds() {
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
