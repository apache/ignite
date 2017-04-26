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

package org.apache.ignite.internal.processors.cache;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
class CacheNodeCommonDiscoveryData implements Serializable {
    /** */
    private final Map<String, CacheData> caches;

    /** */
    private final Map<String, CacheData> templates;

    /** */
    private final Map<String, Map<UUID, Boolean>> clientNodesMap;

    CacheNodeCommonDiscoveryData(Map<String, CacheData> caches,
        Map<String, CacheData> templates,
        Map<String, Map<UUID, Boolean>> clientNodesMap) {
        this.caches = caches;
        this.templates = templates;
        this.clientNodesMap = clientNodesMap;
    }

    Map<String, CacheData> caches() {
        return caches;
    }

    Map<String, CacheData> templates() {
        return templates;
    }

    Map<String, Map<UUID, Boolean>> clientNodesMap() {
        return clientNodesMap;
    }
}
