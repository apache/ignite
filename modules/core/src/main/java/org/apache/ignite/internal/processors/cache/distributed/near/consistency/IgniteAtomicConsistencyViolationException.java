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

package org.apache.ignite.internal.processors.cache.distributed.near.consistency;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 * Atomic consistency violation exception.
 * Has additional fields like 'primary map' and 'on entry repaired callback', because it's impossible to perform repair
 * on locked data (atomics can not be locked), so additional data should be provided to external CAS operation.
 */
public class IgniteAtomicConsistencyViolationException extends IgniteConsistencyViolationException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Entries corrected by a specified strategy. */
    private final Map<KeyCacheObject, EntryGetResult> correctedMap;

    /** Entries located on primary, required to perform CAS operation on inconsistent entries. */
    private final Map<KeyCacheObject, EntryGetResult> primaryMap;

    /** On entry repaired callback, should be called once inconsistent entries replaced by corrected. */
    private final Consumer<Map<KeyCacheObject, EntryGetResult>> callback;

    /**
     * @param correctedMap Corrected map.
     * @param primaryMap Primary map.
     * @param callback Repaired callback.
     */
    public IgniteAtomicConsistencyViolationException(
        Map<KeyCacheObject, EntryGetResult> correctedMap,
        Map<KeyCacheObject, EntryGetResult> primaryMap,
        Consumer<Map<KeyCacheObject, EntryGetResult>> callback) {
        this.correctedMap = Collections.unmodifiableMap(correctedMap);
        this.primaryMap = Collections.unmodifiableMap(primaryMap);
        this.callback = callback;

        assert correctedMap.keySet().equals(primaryMap.keySet());
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> correctedMap() {
        return correctedMap;
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> primaryMap() {
        return primaryMap;
    }

    /**
     *
     */
    public void onRepaired(KeyCacheObject key) {
        callback.accept(Collections.singletonMap(key, correctedMap.get(key)));
    }

    /** {@inheritDoc} */
    @Override public Set<KeyCacheObject> keys() {
        return correctedMap.keySet();
    }
}
