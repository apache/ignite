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

package org.apache.ignite.internal.table.distributed;

import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.table.manager.TableManager;

/**
 * Table Manager that handles inner table lifecycle and provide corresponding API methods.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings({"FieldCanBeLocal", "unused"}) public class TableManagerImpl implements TableManager {
    /** Meta storage service. */
    private final MetaStorageManager metaStorageMgr;

    /** Network cluster. */
    private final ClusterService clusterNetSvc;

    /** Schema manager. */
    private final SchemaManager schemaMgr;

    /** Configuration manager. */
    private final ConfigurationManager configurationMgr;

    /** Raft manager. */
    private final Loza raftMgr;

    /**
     * The constructor.
     *
     * @param configurationMgr Configuration table.
     * @param clusterNetSvc Cluster network service.
     * @param metaStorageMgr MetaStorage manager.
     * @param schemaMgr Schema manager.
     * @param raftMgr Raft manager.
     */
    public TableManagerImpl(
        ConfigurationManager configurationMgr,
        ClusterService clusterNetSvc,
        MetaStorageManager metaStorageMgr,
        SchemaManager schemaMgr,
        Loza raftMgr
    ) {
        this.configurationMgr = configurationMgr;
        this.clusterNetSvc = clusterNetSvc;
        this.metaStorageMgr = metaStorageMgr;
        this.schemaMgr = schemaMgr;
        this.raftMgr = raftMgr;
    }
}
