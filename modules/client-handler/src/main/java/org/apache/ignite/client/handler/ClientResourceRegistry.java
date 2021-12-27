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

package org.apache.ignite.client.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.lang.IgniteException;

/**
 * Per-connection resource registry.
 */
public class ClientResourceRegistry {
    /** Resources. */
    private final Map<Long, ClientResource> res = new ConcurrentHashMap<>();

    /** ID generator. */
    private final AtomicLong idGen = new AtomicLong();

    /**
     * Stores the resource and returns the generated id.
     *
     * @param obj Object.
     * @return Id.
     */
    public long put(ClientResource obj) {
        long id = idGen.incrementAndGet();

        res.put(id, obj);

        return id;
    }

    /**
     * Gets the resource by id.
     *
     * @param id Id.
     * @return Object.
     */
    public ClientResource get(long id) {
        ClientResource res = this.res.get(id);

        if (res == null) {
            throw new IgniteException("Failed to find resource with id: " + id);
        }

        return res;
    }

    /**
     * Removes the resource.
     *
     * @param id Id.
     */
    public ClientResource remove(long id) {
        ClientResource res = this.res.remove(id);

        if (res == null) {
            throw new IgniteException("Failed to find resource with id: " + id);
        }

        return res;
    }

    /**
     * Releases all resources.
     */
    public void clean() {
        IgniteException ex = null;

        for (ClientResource r : res.values()) {
            try {
                r.release();
            } catch (Exception e) {
                if (ex == null) {
                    ex = new IgniteException(e);
                } else {
                    ex.addSuppressed(e);
                }
            }
        }

        res.clear();

        if (ex != null) {
            throw ex;
        }
    }
}
