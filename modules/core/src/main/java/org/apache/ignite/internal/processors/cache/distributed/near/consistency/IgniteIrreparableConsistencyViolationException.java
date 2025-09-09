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

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;

/**
 * Irreparable consistency violation exception.
 */
public class IgniteIrreparableConsistencyViolationException extends IgniteCheckedException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Repairable keys. */
    private final Collection<Object> repairableKeys;

    /** Irreparable keys. */
    private final Collection<Object> irreparableKeys;

    /**
     * @param repairableKeys  Repairable keys.
     * @param irreparableKeys Irreparable keys.
     */
    public IgniteIrreparableConsistencyViolationException(Collection<Object> repairableKeys,
        Collection<Object> irreparableKeys) {
        super("Irreparable distributed cache consistency violation detected.");

        assert irreparableKeys != null && !irreparableKeys.isEmpty() : irreparableKeys;

        this.repairableKeys = repairableKeys != null ? Collections.unmodifiableCollection(repairableKeys) : null;
        this.irreparableKeys = Collections.unmodifiableCollection(irreparableKeys);
    }

    /**
     * Inconsistent keys found but can not be repaired using the specified strategy.
     */
    public Collection<Object> irreparableKeys() {
        return irreparableKeys;
    }

    /**
     * Inconsistent keys found but can be repaired using the specified strategy.
     */
    public Collection<Object> repairableKeys() {
        return repairableKeys;
    }
}
