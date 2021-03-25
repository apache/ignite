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

package org.apache.ignite.lang;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generator of {@link IgniteUuid}.
 */
public class IgniteUuidGenerator {
    /** VM ID. */
    private final UUID globalId;

    /** */
    private final AtomicLong cntGen;

    /**
     * Constructs instance of generator.
     *
     * @param globalId VM ID.
     * @param genStart Start of generation sequence.
     */
    public IgniteUuidGenerator(UUID globalId, long genStart) {
        this.globalId = globalId;

        cntGen = new AtomicLong(genStart);
    }

    /**
     * Gets {@link UUID} associated with local VM.
     *
     * @return {@link UUID} associated with local VM.
     */
    public UUID vmId() {
        return globalId;
    }

    /**
     * Gets last generated local ID.
     *
     * @return Last generated local ID.
     */
    public long lastLocalId() {
        return cntGen.get();
    }

    /**
     * Creates new pseudo-random ID.
     *
     * @return Newly created pseudo-random ID.
     */
    public IgniteUuid randomUuid() {
        return new IgniteUuid(globalId, cntGen.incrementAndGet());
    }

    /**
     * Constructs new {@link IgniteUuid} based on global and local ID portions.
     *
     * @param id UUID instance.
     * @return Newly created pseudo-random ID.
     */
    public IgniteUuid fromUuid(UUID id) {
        Objects.requireNonNull(id, "id must not be null");

        return new IgniteUuid(id, cntGen.getAndIncrement());
    }
}
