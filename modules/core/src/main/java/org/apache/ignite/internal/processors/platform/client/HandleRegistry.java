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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.IgniteException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Server object handle registry.
 */
public class HandleRegistry {
    /** Handles. */
    private final Map<Long, Object> handles = new ConcurrentHashMap<>();

    /** ID generator. */
    private final AtomicLong idGen = new AtomicLong();

    /**
     * Allocates server handle for an object.
     *
     * @param obj Object.
     * @return Handle.
     */
    public long allocate(Object obj) {
        long id = idGen.incrementAndGet();

        handles.put(id, obj);

        return id;
    }

    /**
     * Gets the object by handle.
     *
     * @param hnd Handle.
     * @param <T> Object type.
     * @return Object.
     */
    @SuppressWarnings("unchecked")
    public <T> T get(long hnd) {
        Object obj = handles.get(hnd);

        if (obj == null) {
            throw new IgniteException("Failed to find client object with id: " + hnd);
        }

        return (T) obj;
    }

    /**
     * Releases the handle.
     *
     * @param hnd Handle.
     */
    public void release(long hnd) {
        Object obj = handles.remove(hnd);

        if (obj == null) {
            throw new IgniteException("Failed to find client object with id: " + hnd);
        }
    }

    /**
     * Cleans all handles and closes all AutoCloseables.
     */
    public void clean() {
        for (Map.Entry e : handles.entrySet()) {
            Object val = e.getValue();

            if (val instanceof AutoCloseable) {
                try {
                    ((AutoCloseable)val).close();
                } catch (Exception ex) {
                    throw new IgniteException(ex);
                }
            }
        }
    }
}
