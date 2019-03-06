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

package org.apache.ignite.internal.processors.igfs;

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import org.jetbrains.annotations.Nullable;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IGFS client session. Effectively used to manage lifecycle of opened resources and close them on
 * connection close.
 */
public class IgfsClientSession {
    /** Session resources. */
    private ConcurrentMap<Long, Closeable> rsrcMap = new ConcurrentHashMap<>();

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