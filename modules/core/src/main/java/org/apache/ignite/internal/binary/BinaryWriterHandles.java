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

import java.util.IdentityHashMap;

/**
 * Writer handles. Aimed to delay hash map allocation for some time until it is clearly evident that it is needed.
 */
public class BinaryWriterHandles {
    /** Value denoting null position. */
    public static final int POS_NULL = -1;

    /** Mode: empty. */
    private static final int MODE_EMPTY = 0;

    /** Mode: single object. */
    private static final int MODE_SINGLE = 1;

    /** Mode: multiple objects. */
    private static final int MODE_MULTIPLE = 2;

    /** Data. This is either an object or a map. */
    private Object data;

    /** Position.  */
    private int singlePos;

    /** Mode. */
    private int mode = MODE_EMPTY;

    /**
     * Put object to registry and return previous position (if any).
     *
     * @param obj Object.
     * @param pos Position.
     * @return Old position.
     */
    public int put(Object obj, int pos) {
        assert obj != null;
        assert pos >= 0;

        switch (mode) {
            case MODE_EMPTY:
                this.data = obj;
                this.singlePos = pos;
                this.mode = MODE_SINGLE;

                return POS_NULL;

            case MODE_SINGLE:
                if (this.data == obj)
                    return singlePos;
                else {
                    IdentityHashMap<Object, Integer> newData = new IdentityHashMap<>(2);

                    newData.put(data, singlePos);
                    newData.put(obj, pos);

                    this.data = newData;
                    this.singlePos = -1;
                    this.mode = MODE_MULTIPLE;

                    return POS_NULL;
                }

            default:
                assert mode == MODE_MULTIPLE;

                IdentityHashMap<Object, Integer> data0 = (IdentityHashMap<Object, Integer>)data;

                Integer oldPos = data0.put(obj, pos);

                if (oldPos != null) {
                    // Restore initial position and return it.
                    data0.put(obj, oldPos);

                    return oldPos;
                }
                else
                    return POS_NULL;

        }
    }
}
