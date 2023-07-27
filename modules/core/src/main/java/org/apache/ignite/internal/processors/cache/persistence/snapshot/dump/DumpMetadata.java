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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;

/**
 *
 */
public class DumpMetadata implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 0L;

    /** Unique dump request id. */
    private final UUID rqId;

    /** Snapshot name. */
    @GridToStringInclude
    private final String dumpName;

    /** Consistent id of a node to which this metadata relates. */
    @GridToStringInclude
    private final String consId;

    /** The list of cache groups ids which were included into dump. */
    @GridToStringInclude
    private final List<Integer> grpIds;

    /** The set of affected by dump nodes. */
    @GridToStringInclude
    private final Set<String> nodes;

    /**
     * @param rqId Unique request id.
     * @param dumpName Dump name.
     * @param consId Consistent id of a node to which this metadata relates.
     * @param grpIds The list of cache groups ids which were included into dump.
     * @param nodes The set of affected by dump nodes.
     */
    public DumpMetadata(UUID rqId, String dumpName, String consId, List<Integer> grpIds, Set<String> nodes) {
        this.rqId = rqId;
        this.dumpName = dumpName;
        this.consId = consId;
        this.grpIds = grpIds;
        this.nodes = nodes;
    }

    /** */
    public UUID requestId() {
        return rqId;
    }

    /** */
    public String dumpName() {
        return dumpName;
    }

    /** */
    public String consistentId() {
        return consId;
    }

    /** */
    public List<Integer> groups() {
        return grpIds;
    }

    /** */
    public Set<String> nodes() {
        return nodes;
    }
}
