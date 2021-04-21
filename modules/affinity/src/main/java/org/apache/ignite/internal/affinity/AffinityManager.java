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

package org.apache.ignite.internal.affinity;

import org.apache.ignite.internal.baseline.BaselineManager;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;

/**
 * Affinity manager is responsible for affinity function related logic including calculating affinity assignments.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings({"FieldCanBeLocal", "unused"}) public class AffinityManager {
    /**
     * MetaStorage manager in order to watch private distributed affinity specific configuration,
     * cause ConfigurationManger handles only public configuration.
     */
    private final MetaStorageManager metaStorageMgr;

    /** Configuration manager in order to handle and listen affinity specific configuration.*/
    private final ConfigurationManager configurationMgr;

    /** Baseline manager. */
    private final BaselineManager baselineMgr;

    /**
     * The constructor.
     *
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Meta storage manager.
     */
    public AffinityManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        BaselineManager baselineMgr
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.baselineMgr = baselineMgr;
    }

    // TODO: IGNITE-14237 Affinity function.
    // TODO: IGNITE-14238 Creating and destroying caches.
    // TODO: IGNITE-14235 Provide a minimal cache/table configuration.
}
