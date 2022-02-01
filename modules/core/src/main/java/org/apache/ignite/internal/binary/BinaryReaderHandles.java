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

import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.Nullable;

/**
 * Reader handles.
 */
public class BinaryReaderHandles {
    /** Mode: empty. */
    private static final int MODE_EMPTY = 0;

    /** Mode: single object. */
    private static final int MODE_SINGLE = 1;

    /** Mode: multiple objects. */
    private static final int MODE_MULTIPLE = 2;

    /** Position.  */
    private int singlePos;

    /** Data. This is either an object or a map. */
    private Object data;

    /** Mode. */
    private int mode = MODE_EMPTY;

    /**
     * Get object by position.
     *
     * @param pos Position.
     * @return Object.
     */
    @Nullable public <T> T get(int pos) {
        switch (mode) {
            case MODE_EMPTY:
                return null;

            case MODE_SINGLE:
                 return pos == singlePos ? (T)data : null;

            default:
                assert mode == MODE_MULTIPLE;

                return (T)((Map<Integer, Object>)data).get(pos);
        }
    }

    /**
     * Put object to registry and return previous position (if any).
     *
     * @param pos Position.
     * @param obj Object.
     */
    @SuppressWarnings("unchecked")
    public void put(int pos, Object obj) {
        assert pos >= 0;
        assert obj != null;

        switch (mode) {
            case MODE_EMPTY:
                this.singlePos = pos;
                this.data = obj;
                this.mode = MODE_SINGLE;

                break;

            case MODE_SINGLE:
                Map<Integer, Object> newData = new HashMap(3, 1.0f);

                newData.put(singlePos, data);
                newData.put(pos, obj);

                this.singlePos = -1;
                this.data = newData;
                this.mode = MODE_MULTIPLE;

                break;

            default:
                assert mode == MODE_MULTIPLE;

                Map<Integer, Object> data0 = (Map<Integer, Object>)data;

                data0.put(pos, obj);
        }
    }
}
