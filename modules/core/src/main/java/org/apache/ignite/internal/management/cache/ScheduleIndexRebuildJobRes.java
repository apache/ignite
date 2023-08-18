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

package org.apache.ignite.internal.management.cache;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Result of the ScheduleIndexRebuildJob.
 */
public class ScheduleIndexRebuildJobRes extends IgniteDataTransferObject {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Map cache names -> indexes scheduled for the rebuild. */
    private Map<String, Set<String>> cacheToIndexes;

    /** Names of cache indexes that were not found (cache -> set of indexes). */
    private Map<String, Set<String>> notFoundIndexes;

    /** Names of caches that were not found. */
    private Set<String> notFoundCacheNames;

    /** Names of cache groups that were not found. */
    private Set<String> notFoundGroupNames;

    /**
     * Empty constructor required for Serializable.
     */
    public ScheduleIndexRebuildJobRes() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param cacheToIndexes Map cache names -> indexes scheduled for the rebuild.
     * @param notFoundIndexes Names of cache indexes that were not found (cache -> set of indexes).
     * @param notFoundCacheNames Names of caches that were not found.
     * @param notFoundGroupNames Names of cache groups that were not found.
     */
    public ScheduleIndexRebuildJobRes(
        Map<String, Set<String>> cacheToIndexes,
        Map<String, Set<String>> notFoundIndexes,
        Set<String> notFoundCacheNames,
        Set<String> notFoundGroupNames
    ) {
        this.cacheToIndexes = cacheToIndexes;
        this.notFoundIndexes = notFoundIndexes;
        this.notFoundCacheNames = notFoundCacheNames;
        this.notFoundGroupNames = notFoundGroupNames;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeMap(out, cacheToIndexes);
        U.writeMap(out, notFoundIndexes);
        U.writeCollection(out, notFoundCacheNames);
        U.writeCollection(out, notFoundGroupNames);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        cacheToIndexes = U.readMap(in);
        notFoundIndexes = U.readMap(in);
        notFoundCacheNames = U.readSet(in);
        notFoundGroupNames = U.readSet(in);
    }

    /**
     * @return Map cache names -> indexes scheduled for the rebuild.
     */
    public Map<String, Set<String>> cacheToIndexes() {
        return cacheToIndexes;
    }

    /**
     * @return Names of caches that were not found.
     */
    public Set<String> notFoundCacheNames() {
        return notFoundCacheNames;
    }

    /**
     * @return Names of cache indexes that were not found (cache -> set of indexes).
     */
    public Map<String, Set<String>> notFoundIndexes() {
        return notFoundIndexes;
    }

    /**
     * @return Names of cache groups that were not found.
     */
    public Set<String> notFoundGroupNames() {
        return notFoundGroupNames;
    }
}
