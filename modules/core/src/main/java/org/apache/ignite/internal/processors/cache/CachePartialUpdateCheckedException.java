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
    @SuppressWarnings("unchecked")
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