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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.IgniteException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-connection resource registry.
 */
public class ClientResourceRegistry {
    /** Handles. */
    private final Map<Long, Object> res = new ConcurrentHashMap<>();

    /** ID generator. */
    private final AtomicLong idGen = new AtomicLong();

    /**
     * Allocates server handle for an object.
     *
     * @param obj Object.
     * @return Handle.
     */
    public long put(Object obj) {
        long id = idGen.incrementAndGet();

        res.put(id, obj);

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
        Object obj = res.get(hnd);

        if (obj == null)
            throw new IgniteClientException(
                ClientStatus.RESOURCE_DOES_NOT_EXIST,
                "Failed to find resource with id: " + hnd
            );

        return (T) obj;
    }

    /**
     * Releases the handle.
     *
     * @param hnd Handle.
     */
    public void release(long hnd) {
        Object obj = res.remove(hnd);

        if (obj == null)
            throw new IgniteClientException(
                ClientStatus.RESOURCE_DOES_NOT_EXIST,
                "Failed to find resource with id: " + hnd
            );

        closeIfNeeded(obj);
    }

    /**
     * Cleans all handles and closes all ClientCloseableResources.
     */
    public void clean() {
        for (Map.Entry e : res.entrySet())
            closeIfNeeded(e.getValue());
    }

    /**
     * Close resource if needed.
     *
     * @param res Resource.
     */
    private static void closeIfNeeded(Object res) {
        if (res instanceof ClientCloseableResource)
            ((ClientCloseableResource)res).close();
    }
}
