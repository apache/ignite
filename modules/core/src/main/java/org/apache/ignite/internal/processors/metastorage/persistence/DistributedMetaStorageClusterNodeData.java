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

package org.apache.ignite.internal.processors.metastorage.persistence;

import java.io.Serializable;

/**
 * Distributed metastorage data that cluster sends to joining node.
 */
@SuppressWarnings({"PublicField", "AssignmentOrReturnOfFieldWithMutableType"})
class DistributedMetaStorageClusterNodeData implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Distributed metastorage version of cluster. If {@link #fullData} is not null then this version corresponds to
     * its content.
     */
    public final DistributedMetaStorageVersion ver;

    /**
     * Full data is sent if there's not enough history items on local node.
     */
    public final DistributedMetaStorageKeyValuePair[] fullData;

    /**
     * Required updates for joining nodes or full available history of local node if {@link #fullData} is
     * not {@code null}.
     */
    public final DistributedMetaStorageHistoryItem[] hist;

    /**
     * Additional updates. Makes sence only if {@link #fullData} is not {@code null}.
     */
    public DistributedMetaStorageHistoryItem[] updates;

    /** */
    public DistributedMetaStorageClusterNodeData(
        DistributedMetaStorageVersion ver,
        DistributedMetaStorageKeyValuePair[] fullData,
        DistributedMetaStorageHistoryItem[] hist,
        DistributedMetaStorageHistoryItem[] updates
    ) {
        assert ver != null;
        assert fullData == null || hist != null;

        this.fullData = fullData;
        this.ver = ver;
        this.hist = hist;
        this.updates = updates;
    }
}
