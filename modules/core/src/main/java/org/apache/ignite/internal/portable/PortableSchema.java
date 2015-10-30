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
    /** Inline flag. */
    private final boolean inline;

    /** Map with offsets. */
    private final HashMap<Integer, Integer> map;

    /** ID 1. */
    private final int id1;

    /** Offset 1. */
    private final int offset1;

    /** ID 2. */
    private final int id2;

    /** Offset 2. */
    private final int offset2;

    /** ID 3. */
    private final int id3;

    /** Offset 3. */
    private final int offset3;

    /** ID 4. */
    private final int id4;

    /** Offset 4. */
    private final int offset4;

    /** ID 1. */
    private final int id5;

    /** Offset 1. */
    private final int offset5;

    /** ID 2. */
    private final int id6;

    /** Offset 2. */
    private final int offset6;

    /** ID 3. */
    private final int id7;

    /** Offset 3. */
    private final int offset7;

    /** ID 4. */
    private final int id8;

    /** Offset 4. */
    private final int offset8;

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
                id1 = entry.getKey();
                offset1 = entry.getValue();
            }
            else{
                id1 = 0;
                offset1 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id2 = entry.getKey();
                offset2 = entry.getValue();
            }
            else{
                id2 = 0;
                offset2 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id3 = entry.getKey();
                offset3 = entry.getValue();
            }
            else{
                id3 = 0;
                offset3 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id4 = entry.getKey();
                offset4 = entry.getValue();
            }
            else{
                id4 = 0;
                offset4 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id5 = entry.getKey();
                offset5 = entry.getValue();
            }
            else{
                id5 = 0;
                offset5 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id6 = entry.getKey();
                offset6 = entry.getValue();
            }
            else{
                id6 = 0;
                offset6 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id7 = entry.getKey();
                offset7 = entry.getValue();
            }
            else{
                id7 = 0;
                offset7 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id8 = entry.getKey();
                offset8 = entry.getValue();
            }
            else{
                id8 = 0;
                offset8 = 0;
            }

            map = null;
        }
        else {
            inline = false;

            id1 = id2 = id3 = id4 = id5 = id6 = id7 = id8 = 0;
            offset1 = offset2 = offset3 = offset4 = offset5 = offset6 = offset7 = offset8 = 0;

            map = new HashMap<>(vals);
        }
    }

    /**
     * Get offset for the given field ID.
     *
     * @param id Field ID.
     * @return Offset or {@code 0} if there is no such field.
     */
    public int offset(int id) {
        if (inline) {
            if (id == id1)
                return offset1;

            if (id == id2)
                return offset2;

            if (id == id3)
                return offset3;

            if (id == id4)
                return offset4;

            if (id == id5)
                return offset5;

            if (id == id6)
                return offset6;

            if (id == id7)
                return offset7;

            if (id == id8)
                return offset8;

            return 0;
        }
        else {
            Integer off = map.get(id);

            return off != null ? off : 0;
        }
    }
}
