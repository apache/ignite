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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.util.Arrays;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Represents an index key that stores as Java Object.
 *
 * {@link IndexKeyType#JAVA_OBJECT}.
 */
public abstract class JavaObjectIndexKey implements IndexKey {
    /** {@inheritDoc} */
    @Override public int hashCode() {
        return key().hashCode();
    }

    /** {@inheritDoc} */
    @Override public IndexKeyType type() {
        return IndexKeyType.JAVA_OBJECT;
    }

    /** {@inheritDoc} */
    @Override public int compare(IndexKey o) {
        Object o1 = key();
        Object o2 = o.key();

        boolean isComparable = o1 instanceof Comparable;
        boolean otherIsComparable = o2 instanceof Comparable;

        // Keep old logic there and below.
        if (isComparable && otherIsComparable && haveCommonComparableSuperclass(o1.getClass(), o2.getClass()))
            return ((Comparable)o1).compareTo(o2);

        else if (o1.getClass() != o2.getClass()) {
            if (isComparable != otherIsComparable)
                return isComparable ? -1 : 1;
            else
                return o1.getClass().getName().compareTo(o2.getClass().getName());

        }
        else {
            int h1 = o1.hashCode();
            int h2 = o2.hashCode();

            if (h1 == h2) {
                return o1.equals(o2)
                    ? 0
                    : Integer.signum(F.compareArrays(bytesNoCopy(), ((JavaObjectIndexKey)o).bytesNoCopy()));
            }
            else
                return h1 > h2 ? 1 : -1;
        }
    }

    /** */
    static boolean haveCommonComparableSuperclass(Class<?> cls0, Class<?> cls1) {
        if (cls0 != cls1 && !cls0.isAssignableFrom(cls1) && !cls1.isAssignableFrom(cls0)) {
            Class<?> supCls0;
            do {
                supCls0 = cls0;
                cls0 = cls0.getSuperclass();
            } while (Comparable.class.isAssignableFrom(cls0));

            Class<?> supCls1;
            do {
                supCls1 = cls1;
                cls1 = cls1.getSuperclass();
            } while (Comparable.class.isAssignableFrom(cls1));

            return supCls0 == supCls1;
        }
        else
            return true;
    }

    /**
     * @return Cached byte array that represents a Java object.
     */
    public abstract byte[] bytesNoCopy();

    /** Empty byte array. */
    private static final byte[] EMPTY_BYTES = new byte[0];

    /**
     * @return Byte array that represents a Java object.
     */
    public byte[] bytes() {
        byte[] bytes = bytesNoCopy();

        if (bytes == null)
            return null;
        else {
            int len = bytes.length;
            return len == 0 ? EMPTY_BYTES : Arrays.copyOf(bytes, len);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return key().toString();
    }

//    /** {@inheritDoc} */
//    @Override public boolean equals(Object o) {
//        return o instanceof JavaObjectIndexKey && compare((IndexKey)o) == 0;
//    }
}
