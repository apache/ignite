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

package org.apache.ignite.internal.processors.igfs;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

/**
 * IGFS client session. Effectively used to manage lifecycle of opened resources and close them on
 * connection close.
 */
public class IgfsClientSession {
    /** Session resources. */
    private ConcurrentMap<Long, Closeable> rsrcMap = new ConcurrentHashMap8<>();

    /**
     * Registers resource within this session.
     *
     * @param rsrcId Resource id.
     * @param rsrc Resource to register.
     */
    public boolean registerResource(long rsrcId, Closeable rsrc) {
        Object old = rsrcMap.putIfAbsent(rsrcId, rsrc);

        return old == null;
    }

    /**
     * Gets registered resource by ID.
     *
     * @param rsrcId Resource ID.
     * @return Resource or {@code null} if resource was not found.
     */
    @Nullable public <T> T resource(Long rsrcId) {
        return (T)rsrcMap.get(rsrcId);
    }

    /**
     * Unregister previously registered resource.
     *
     * @param rsrcId Resource ID.
     * @param rsrc Resource to unregister.
     * @return {@code True} if resource was unregistered, {@code false} if no resource
     *      is associated with this ID or other resource is associated with this ID.
     */
    public boolean unregisterResource(Long rsrcId, Closeable rsrc) {
        return rsrcMap.remove(rsrcId, rsrc);
    }

    /**
     * @return Registered resources iterator.
     */
    public Iterator<Closeable> registeredResources() {
        return rsrcMap.values().iterator();
    }
}