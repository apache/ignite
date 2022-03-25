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
import java.util.function.Consumer;
import org.apache.ignite.internal.processors.cache.EntryGetResult;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;

/**
 *
 */
public class IgniteAtomicConsistencyViolationException extends IgniteConsistencyViolationException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Fixed map. */
    private final Map<KeyCacheObject, EntryGetResult> fixedMap;

    /** Primary map. */
    private final Map<KeyCacheObject, EntryGetResult> primaryMap;

    /** On fixed callback. */
    private final Consumer<Map<KeyCacheObject, EntryGetResult>> callback;

    /**
     *  @param fixedMap Fixed map.
     * @param primaryMap Primary map.
     * @param callback Fixed callback.
     */
    public IgniteAtomicConsistencyViolationException(
        Map<KeyCacheObject, EntryGetResult> fixedMap,
        Map<KeyCacheObject, EntryGetResult> primaryMap,
        Consumer<Map<KeyCacheObject, EntryGetResult>> callback) {
        super(fixedMap.keySet());
        this.fixedMap = Collections.unmodifiableMap(fixedMap);
        this.primaryMap = Collections.unmodifiableMap(primaryMap);
        this.callback = callback;

        assert fixedMap.keySet().equals(primaryMap.keySet());
    }

    /**
     *
     */
    public Map<KeyCacheObject, EntryGetResult> fixedMap() {
        return fixedMap;
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
    public void onFixed(KeyCacheObject key) {
        callback.accept(Collections.singletonMap(key, fixedMap.get(key)));
    }
}
