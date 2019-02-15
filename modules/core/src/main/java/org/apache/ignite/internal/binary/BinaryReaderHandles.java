/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.binary;

import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

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
    @SuppressWarnings("unchecked")
    public @Nullable <T> T get(int pos) {
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
