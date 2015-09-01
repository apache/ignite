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

package org.apache.ignite.internal.util;

import java.util.AbstractSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public class GridLeanIdentitySet<T> extends AbstractSet<T> {
    /** */
    private static final int MAX_ARR_SIZE = 8;

    /** */
    private Object data;

    /** */
    private int size;

    /** {@inheritDoc} */
    @Override public boolean contains(Object o) {
        if (size == 0)
            return false;

        if (size == 1)
            return o == data;

        if (size <= MAX_ARR_SIZE) {
            Object[] arr = (Object[])data;

            for (int i = 0; i < size; i++) {
                if (arr[i] == o)
                    return true;
            }

            return false;
        }

        return ((Map<Object, Boolean>)data).containsKey(o);
    }

    /** {@inheritDoc} */
    @Override public boolean add(T t) {
        if (size > MAX_ARR_SIZE) {
            if (((Map<Object, Boolean>)data).put(t, Boolean.TRUE) == null) {
                size++;

                return true;
            }

            return false;
        }

        if (contains(t))
            return false;

        if (size == 0)
            data = t;
        else if (size == 1) {
            Object[] arr = new Object[MAX_ARR_SIZE];

            arr[0] = data;
            arr[1] = t;

            data = arr;
        }
        else if (size < MAX_ARR_SIZE)
            ((Object[])data)[size] = t;
        else if (size == MAX_ARR_SIZE) {
            Map<Object, Boolean> map = new IdentityHashMap<>();

            for (Object o : (Object[])data)
                map.put(o, Boolean.TRUE);

            map.put(t, Boolean.TRUE);

            assert map.size() == size + 1;

            data = map;
        }

        size++;

        return true;
    }

    /** {@inheritDoc} */
    @Override public void clear() {
        data = null;
        size = 0;
    }

    /** {@inheritDoc} */
    @Override public Iterator<T> iterator() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }
}