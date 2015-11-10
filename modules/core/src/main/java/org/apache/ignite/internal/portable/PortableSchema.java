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

package org.apache.ignite.internal.portable;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Schema describing portable object content. We rely on the following assumptions:
 * - When amount of fields in the object is low, it is better to inline these values into int fields thus allowing
 * for quick comparisons performed within already fetched L1 cache line.
 * - When there are more fields, we store them inside a hash map.
 */
public class PortableSchema {
    /** Order returned if field is not found. */
    public static final int ORDER_NOT_FOUND = -1;

    /** Inline flag. */
    private final boolean inline;

    /** Map with offsets. */
    private final HashMap<Integer, Integer> map;

    /** ID 1. */
    private final int id0;

    /** ID 2. */
    private final int id1;

    /** ID 3. */
    private final int id2;

    /** ID 4. */
    private final int id3;

    /** ID 1. */
    private final int id4;

    /** ID 2. */
    private final int id5;

    /** ID 3. */
    private final int id6;

    /** ID 4. */
    private final int id7;

    /**
     * Constructor.
     *
     * @param vals Values.
     */
    public PortableSchema(LinkedHashMap<Integer, Integer> vals) {
        if (vals.size() <= 8) {
            inline = true;

            Iterator<Map.Entry<Integer, Integer>> iter = vals.entrySet().iterator();

            Map.Entry<Integer, Integer> entry = iter.hasNext() ? iter.next() : null;

            if (entry != null) {
                id0 = entry.getKey();

                assert entry.getValue() == 0;
            }
            else
                id0 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id1 = entry.getKey();

                assert entry.getValue() == 1;
            }
            else
                id1 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id2 = entry.getKey();

                assert entry.getValue() == 2;
            }
            else
                id2 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id3 = entry.getKey();

                assert entry.getValue() == 3;
            }
            else
                id3 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id4 = entry.getKey();

                assert entry.getValue() == 4;
            }
            else
                id4 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id5 = entry.getKey();

                assert entry.getValue() == 5;
            }
            else
                id5 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id6 = entry.getKey();

                assert entry.getValue() == 6;
            }
            else
                id6 = 0;

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id7 = entry.getKey();

                assert entry.getValue() == 7;
            }
            else
                id7 = 0;

            map = null;
        }
        else {
            inline = false;

            id0 = id1 = id2 = id3 = id4 = id5 = id6 = id7 = 0;

            map = new HashMap<>(vals);
        }
    }

    /**
     * Get field position in footer by schema ID.
     *
     * @param id Field ID.
     * @return Offset or {@code 0} if there is no such field.
     */
    public int order(int id) {
        if (inline) {
            if (id == id0)
                return 0;

            if (id == id1)
                return 1;

            if (id == id2)
                return 2;

            if (id == id3)
                return 3;

            if (id == id4)
                return 4;

            if (id == id5)
                return 5;

            if (id == id6)
                return 6;

            if (id == id7)
                return 7;

            return ORDER_NOT_FOUND;
        }
        else {
            Integer order = map.get(id);

            return order != null ? order : ORDER_NOT_FOUND;
        }
    }
}
