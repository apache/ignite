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

package org.apache.ignite.internal.cache.query.index.sorted.client;

import java.util.LinkedHashMap;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;

/**
 * Define index for filtered or client node.
 */
public class ClientIndexDefinition implements IndexDefinition {
    /** */
    private final int cfgInlineSize;

    /** */
    private final int maxInlineSize;

    /** */
    private final IndexName idxName;

    /** */
    private final LinkedHashMap<String, IndexKeyDefinition> keyDefs;

    /** */
    public ClientIndexDefinition(
        IndexName idxName,
        LinkedHashMap<String, IndexKeyDefinition> keyDefs,
        int cfgInlineSize,
        int maxInlineSize
    ) {
        this.idxName = idxName;
        this.cfgInlineSize = cfgInlineSize;
        this.maxInlineSize = maxInlineSize;
        this.keyDefs = keyDefs;
    }

    /** */
    public int getCfgInlineSize() {
        return cfgInlineSize;
    }

    /** */
    public int getMaxInlineSize() {
        return maxInlineSize;
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
