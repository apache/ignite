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
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
* Reader context.
*/
class PortableReaderContext {
    /** */
    private Object oHandles;

    /** */
    private Map<Integer, BinaryObject> poHandles;

    /**
     * @param handle Handle.
     * @param obj Object.
     */
    @SuppressWarnings("unchecked")
    void setObjectHandler(int handle, Object obj) {
        assert obj != null;

        if (oHandles == null)
            oHandles = new IgniteBiTuple(handle, obj);
        else if (oHandles instanceof IgniteBiTuple) {
            Map map = new HashMap(3, 1.0f);

            IgniteBiTuple t = (IgniteBiTuple)oHandles;

            map.put(t.getKey(), t.getValue());
            map.put(handle, obj);

            oHandles = map;
        }
        else
            ((Map)oHandles).put(handle, obj);
    }

    /**
     * @param handle Handle.
     * @param po Portable object.
     */
    void setPortableHandler(int handle, BinaryObject po) {
        assert po != null;

        if (poHandles == null)
            poHandles = new HashMap<>(3, 1.0f);

        poHandles.put(handle, po);
    }

    /**
     * @param handle Handle.
     * @return Object.
     */
    @Nullable Object getObjectByHandle(int handle) {
        if (oHandles != null) {
            if (oHandles instanceof IgniteBiTuple) {
                IgniteBiTuple t = (IgniteBiTuple)oHandles;

                if ((int)t.get1() == handle)
                    return t.get2();
            }
            else
                return ((Map)oHandles).get(handle);
        }

        return null;
    }

    /**
     * @param handle Handle.
     * @return Object.
     */
    @Nullable BinaryObject getPortableByHandle(int handle) {
        return poHandles != null ? poHandles.get(handle) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableReaderContext.class, this);
    }
}
