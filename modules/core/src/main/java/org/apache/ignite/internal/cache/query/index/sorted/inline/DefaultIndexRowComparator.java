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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import org.apache.ignite.internal.cache.query.index.sorted.inline.io.IndexSearchRow;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.keys.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * Default row comparator. Consider that every index key extends Comparable interface.
 * Does not support comparation of different key types.
 */
public class DefaultIndexRowComparator implements IndexRowComparator {
    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, Object v, int curType) {
        return COMPARE_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexSearchRow left, IndexSearchRow right, int idx) {
        Object v1 = left.getKey(idx);
        Object v2 = right.getKey(idx);
        return Integer.signum(compare(v1, v2));
    }

    /** */
    private int compare(Object o1, Object o2) {
        assert o1 instanceof Comparable: o1;
        assert o2 instanceof Comparable: o2;

        assert haveCommonComparableSuperclass(o1.getClass(), o2.getClass());

        Comparable<Object> c1 = (Comparable<Object>)o1;

        return c1.compareTo(o2);
    }

    /** */
    private static boolean haveCommonComparableSuperclass(Class<?> clazz0, Class<?> clazz1) {
        if (clazz0 != clazz1 && !clazz0.isAssignableFrom(clazz1) && !clazz1.isAssignableFrom(clazz0)) {
            Class<?> baseClazz0;
            do {
                baseClazz0 = clazz0;
                clazz0 = clazz0.getSuperclass();
            } while(Comparable.class.isAssignableFrom(clazz0));

            Class<?> baseClazz1;
            do {
                baseClazz1 = clazz1;
                clazz1 = clazz1.getSuperclass();
            } while(Comparable.class.isAssignableFrom(clazz1));

            return baseClazz0 == baseClazz1;
        } else
            return true;
    }
}
