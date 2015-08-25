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

import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.portable.*;

import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Reader context.
 */
class PortableReaderContext {
    /** */
    private Map<Integer, Object> oHandles;

    /** */
    private Map<Integer, PortableObject> poHandles;

    /**
     * @param handle Handle.
     * @param obj Object.
     */
    void setObjectHandler(int handle, Object obj) {
        assert obj != null;

        if (oHandles == null)
            oHandles = new HashMap<>(3, 1.0f);

        oHandles.put(handle, obj);
    }

    /**
     * @param handle Handle.
     * @param po Portable object.
     */
    void setPortableHandler(int handle, PortableObject po) {
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
        return oHandles != null ? oHandles.get(handle) : null;
    }

    /**
     * @param handle Handle.
     * @return Object.
     */
    @Nullable PortableObject getPortableByHandle(int handle) {
        return poHandles != null ? poHandles.get(handle) : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PortableReaderContext.class, this);
    }
}
