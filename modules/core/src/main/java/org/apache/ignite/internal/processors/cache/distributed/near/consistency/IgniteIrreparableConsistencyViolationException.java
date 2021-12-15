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

/**
 * Irreparable consistency violation exception.
 */
public class IgniteIrreparableConsistencyViolationException extends IgniteConsistencyViolationException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Irreparable keys. */
    private final Collection<?> irreparableKeys;

    /**
     * @param keys            Keys.
     * @param irreparableKeys Irreparable keys.
     */
    public IgniteIrreparableConsistencyViolationException(Collection<?> keys, Collection<?> irreparableKeys) {
        super(keys);

        assert irreparableKeys != null && !irreparableKeys.isEmpty() : irreparableKeys;

        this.irreparableKeys = irreparableKeys;
    }

    /**
     * Inconsistent keys found but can not be fixed using the specified strategy.
     */
    public Collection<?> irreparableKeys() {
        return irreparableKeys;
    }
}
