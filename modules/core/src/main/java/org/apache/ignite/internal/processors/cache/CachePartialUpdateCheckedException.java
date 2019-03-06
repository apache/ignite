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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

/**
 * Exception thrown from non-transactional cache in case when update succeeded only partially.
 * One can get list of keys for which update failed with method {@link #failedKeys()}.
 */
public class CachePartialUpdateCheckedException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Failed keys. */
    private final Collection<Object> failedKeys = new ArrayList<>();

    /** */
    private transient AffinityTopologyVersion topVer;

    /**
     * @param msg Error message.
     */
    public CachePartialUpdateCheckedException(String msg) {
        super(msg);
    }

    /**
     * Gets collection of failed keys.
     * @return Collection of failed keys.
     */
    public synchronized <K> Collection<K> failedKeys() {
        return new LinkedHashSet<>((Collection<K>)failedKeys);
    }

    /**
     * @param failedKeys Failed keys.
     * @param err Error.
     * @param topVer Topology version for failed update.
     */
    public synchronized void add(Collection<?> failedKeys, Throwable err, AffinityTopologyVersion topVer) {
        if (topVer != null) {
            AffinityTopologyVersion topVer0 = this.topVer;

            if (topVer0 == null || topVer.compareTo(topVer0) > 0)
                this.topVer = topVer;
        }

        this.failedKeys.addAll(failedKeys);

        addSuppressed(err);
    }

    /**
     * @return Topology version.
     */
    public synchronized AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    /**
     * @param failedKeys Failed keys.
     * @param err Error.
     */
    public synchronized void add(Collection<?> failedKeys, Throwable err) {
        add(failedKeys, err, null);
    }

    /** {@inheritDoc} */
    @Override public String getMessage() {
        return super.getMessage() + ": " + failedKeys;
    }
}