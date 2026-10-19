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

import org.jetbrains.annotations.Nullable;

/**
 * Simple holder for handles.
 */
class BinaryReaderHandlesHolder {
    /** Reader context which is constantly passed between objects. */
    BinaryReaderHandles hnds;

    /**
     * @param hnds Handles.
     */
    BinaryReaderHandlesHolder(@Nullable BinaryReaderHandles hnds) {
        this.hnds = hnds;
    }

    /**
     * Set handle.
     *
     * @param obj Object.
     * @param pos Position.
     */
    final void setHandle(Object obj, int pos) {
        handles().put(pos, obj);
    }

    /**
     * Get handle.
     *
     * @param pos Position.
     * @return Handle.
     */
    final Object getHandle(int pos) {
        return hnds != null ? hnds.get(pos) : null;
    }

    /**
     * Get all handles.
     *
     * @return Handles.
     */
    public final BinaryReaderHandles handles() {
        if (hnds == null)
            hnds = new BinaryReaderHandles();

        return hnds;
    }

    /** @return {@code True} if no handles were registered. */
    final boolean isEmpty() {
        return hnds == null || hnds.isEmpty();
    }
}
