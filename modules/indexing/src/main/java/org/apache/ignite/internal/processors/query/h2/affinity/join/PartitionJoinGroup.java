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

package org.apache.ignite.internal.processors.query.h2.affinity.join;

import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;

public class PartitionJoinGroup {
    /** Tables within a group. */
    private final Collection<PartitionJoinTable> tbls = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Tables that were left joined to the group (i.e. these are tables that were on the right side of LJ. */
    private final Collection<PartitionJoinTable> outerTbls = Collections.newSetFromMap(new IdentityHashMap<>());

    /** Affinity function identifier. */
    private final PartitionJoinAffinityIdentifier affIdentifier;

    /** Whether this is replicated group. */
    private final boolean replicated;

    /**
     * Constructor.
     *
     * @param affIdentifier Affinity identifier.
     * @param replicated Replicated flag.
     */
    public PartitionJoinGroup(PartitionJoinAffinityIdentifier affIdentifier, boolean replicated) {
        this.affIdentifier = affIdentifier;
        this.replicated = replicated;
    }

    public Collection<PartitionJoinTable> tables() {
        return tbls;
    }

    public Collection<PartitionJoinTable> outerTables() {
        return outerTbls;
    }

    public PartitionJoinAffinityIdentifier affinityIdentifer() {
        return affIdentifier;
    }

    public boolean replicated() {
        return replicated;
    }
}
