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
    private final int pos1;

    /** ID 2. */
    private final int id2;

    /** Offset 2. */
    private final int pos2;

    /** ID 3. */
    private final int id3;

    /** Offset 3. */
    private final int pos3;

    /** ID 4. */
    private final int id4;

    /** Offset 4. */
    private final int pos4;

    /** ID 1. */
    private final int id5;

    /** Offset 1. */
    private final int pos5;

    /** ID 2. */
    private final int id6;

    /** Offset 2. */
    private final int pos6;

    /** ID 3. */
    private final int id7;

    /** Offset 3. */
    private final int pos7;

    /** ID 4. */
    private final int id8;

    /** Offset 4. */
    private final int pos8;

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
                pos1 = entry.getValue();
            }
            else{
                id1 = 0;
                pos1 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id2 = entry.getKey();
                pos2 = entry.getValue();
            }
            else{
                id2 = 0;
                pos2 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id3 = entry.getKey();
                pos3 = entry.getValue();
            }
            else{
                id3 = 0;
                pos3 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id4 = entry.getKey();
                pos4 = entry.getValue();
            }
            else{
                id4 = 0;
                pos4 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id5 = entry.getKey();
                pos5 = entry.getValue();
            }
            else{
                id5 = 0;
                pos5 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id6 = entry.getKey();
                pos6 = entry.getValue();
            }
            else{
                id6 = 0;
                pos6 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id7 = entry.getKey();
                pos7 = entry.getValue();
            }
            else{
                id7 = 0;
                pos7 = 0;
            }

            if ((entry = iter.hasNext() ? iter.next() : null) != null) {
                id8 = entry.getKey();
                pos8 = entry.getValue();
            }
            else{
                id8 = 0;
                pos8 = 0;
            }

            map = null;
        }
        else {
            inline = false;

            id1 = id2 = id3 = id4 = id5 = id6 = id7 = id8 = 0;
            pos1 = pos2 = pos3 = pos4 = pos5 = pos6 = pos7 = pos8 = 0;

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
            if (id == id1)
                return pos1;

            if (id == id2)
                return pos2;

            if (id == id3)
                return pos3;

            if (id == id4)
                return pos4;

            if (id == id5)
                return pos5;

            if (id == id6)
                return pos6;

            if (id == id7)
                return pos7;

            if (id == id8)
                return pos8;

            return 0;
        }
        else {
            Integer off = map.get(id);

            return off != null ? off : 0;
        }
    }
}
