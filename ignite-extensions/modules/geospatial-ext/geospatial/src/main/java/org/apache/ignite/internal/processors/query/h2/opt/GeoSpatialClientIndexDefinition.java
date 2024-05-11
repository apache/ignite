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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.util.LinkedHashMap;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;

/**
 * Definition of Geo-Spatial client index.
 */
public class GeoSpatialClientIndexDefinition implements IndexDefinition {
    /** */
    private final LinkedHashMap<String, IndexKeyDefinition> keyDefs;

    /** */
    private final IndexName idxName;

    /** */
    public GeoSpatialClientIndexDefinition(
        IndexName idxName,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs
    ) {
        this.idxName = idxName;
        this.keyDefs = keyDefs;
    }

    /** {@inheritDoc} */
    @Override public IndexName idxName() {
        return idxName;
    }

    /** {@inheritDoc} */
    @Override public LinkedHashMap<String, IndexKeyDefinition> indexKeyDefinitions() {
        return keyDefs;
    }
}
