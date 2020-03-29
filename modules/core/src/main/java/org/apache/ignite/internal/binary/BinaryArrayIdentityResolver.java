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

import java.util.Arrays;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Identity resolver implementation which compares raw array content of the binary object.
 * <p>
 * Hash code is calculated in the same way as {@link Arrays#hashCode(byte[])} does.
 */
public class BinaryArrayIdentityResolver extends BinaryAbstractIdentityResolver {
    /** Singleton instance */
    private static final BinaryArrayIdentityResolver INSTANCE = new BinaryArrayIdentityResolver();

    /**
     * Get singleton instance.
     *
     * @return Singleton instance.
     */
    public static BinaryArrayIdentityResolver instance() {
        return INSTANCE;
    }

    /**
     * Default constructor.
     */
    public BinaryArrayIdentityResolver() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected int hashCode0(BinaryObject obj) {
        int hash = 1;

        if (obj instanceof BinaryObjectExImpl) {
            BinaryObjectExImpl ex = (BinaryObjectExImpl)obj;

            int start = ex.dataStartOffset();
            int end = ex.footerStartOffset();

            if (ex.hasArray()) {
                // Handle heap object.
                byte[] data = ex.array();

                for (int i = start; i < end; i++)
                    hash = 31 * hash + data[i];
            }
            else {
                // Handle offheap object.
                long ptr = ex.offheapAddress();

                for (int i = start; i < end; i++)
                    hash = 31 * hash + BinaryPrimitives.readByte(ptr, i);
            }
        }
        else if (obj instanceof BinaryEnumObjectImpl) {
            int ord = obj.enumOrdinal();

            // Construct hash as if it was an int serialized in little-endian form.
            hash = 31 * hash + (ord & 0x000000FF);
            hash = 31 * hash + (ord & 0x0000FF00);
            hash = 31 * hash + (ord & 0x00FF0000);
            hash = 31 * hash + (ord & 0xFF000000);
        }
        else
            throw new BinaryObjectException("Array identity resolver cannot be used with provided BinaryObject " +
                "implementation: " + obj.getClass().getName());

        return hash;
    }

    /** {@inheritDoc} */
    @Override protected boolean equals0(BinaryObject o1, BinaryObject o2) {
        if (o1 instanceof BinaryObjectEx && o2 instanceof BinaryObjectEx) {
            BinaryObjectEx ex1 = (BinaryObjectEx)o1;
            BinaryObjectEx ex2 = (BinaryObjectEx)o2;

            if (ex1.typeId() != ex2.typeId())
                return false;

            if (ex1 instanceof BinaryObjectExImpl) {
                // Handle regular object.
                assert ex2 instanceof BinaryObjectExImpl;

                BinaryObjectExImpl exx1 = (BinaryObjectExImpl)ex1;
                BinaryObjectExImpl exx2 = (BinaryObjectExImpl)ex2;

                if (exx1.hasArray())
                    return exx2.hasArray() ? equalsHeap(exx1, exx2) : equalsHeapOffheap(exx1, exx2);
                else
                    return exx2.hasArray() ? equalsHeapOffheap(exx2, exx1) : equalsOffheap(exx1, exx2);
            }
            else {
                // Handle enums.
                assert ex1 instanceof BinaryEnumObjectImpl;
                assert ex2 instanceof BinaryEnumObjectImpl;

                return ex1.enumOrdinal() == ex2.enumOrdinal();
            }
        }

        BinaryObject o = o1 instanceof BinaryObjectEx ? o2 : o1;

        throw new BinaryObjectException("Array identity resolver cannot be used with provided BinaryObject " +
            "implementation: " + o.getClass().getName());
    }

    /**
     * Compare two heap objects.
     *
     * @param o1 Object 1.
     * @param o2 Object 2.
     * @return Result.
     */
    private static boolean equalsHeap(BinaryObjectExImpl o1, BinaryObjectExImpl o2) {
        byte[] arr1 = o1.array();
        byte[] arr2 = o2.array();

        assert arr1 != null && arr2 != null;

        int i = o1.dataStartOffset();
        int j = o2.dataStartOffset();

        int end = o1.footerStartOffset();

        // Check length.
        if (end - i != o2.footerStartOffset() - j)
            return false;

        for (; i < end; i++, j++) {
            if (arr1[i] != arr2[j])
                return false;
        }

        return true;
    }

    /**
     * Compare heap and offheap objects.
     *
     * @param o1 Object 1 (heap).
     * @param o2 Object 2 (offheap).
     * @return Result.
     */
    private static boolean equalsHeapOffheap(BinaryObjectExImpl o1, BinaryObjectExImpl o2) {
        byte[] arr1 = o1.array();
        long ptr2 = o2.offheapAddress();

        assert arr1 != null && ptr2 != 0;

        int i = o1.dataStartOffset();
        int j = o2.dataStartOffset();

        int end = o1.footerStartOffset();

        // Check length.
        if (end - i != o2.footerStartOffset() - j)
            return false;

        for (; i < end; i++, j++) {
            if (arr1[i] != BinaryPrimitives.readByte(ptr2, j))
                return false;
        }

        return true;
    }

    /**
     * Compare two offheap objects.
     *
     * @param o1 Object 1.
     * @param o2 Object 2.
     * @return Result.
     */
    private static boolean equalsOffheap(BinaryObjectExImpl o1, BinaryObjectExImpl o2) {
        long ptr1 = o1.offheapAddress();
        long ptr2 = o2.offheapAddress();

        assert ptr1 != 0 && ptr2 != 0;

        int i = o1.dataStartOffset();
        int j = o2.dataStartOffset();

        int end = o1.footerStartOffset();

        // Check length.
        if (end - i != o2.footerStartOffset() - j)
            return false;

        for (; i < end; i++, j++) {
            if (BinaryPrimitives.readByte(ptr1, i) != BinaryPrimitives.readByte(ptr2, j))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(BinaryArrayIdentityResolver.class, this);
    }
}
