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

package org.apache.ignite.internal.cache.query.index;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Metadata for IndexQuery response. This information is required to be sent to a node that initiated a query.
 * Thick client nodes may have irrelevant information about index structure, {@link MetaPageInfo}.
 */
public class IndexQueryResultMeta implements Message {
    /** Index key settings. */
    @Order(0)
    private IndexKeyTypeSettings keyTypeSettings;

    /** Index names order holder. Should be serialized before the definitions. */
    @Order(value = 1, method = "orderredIndexNames")
    private List<String> idxNames;

    /** Index definitions serialization holder. Should be serialized after the names. */
    @Order(value = 2, method = "orderredIndexDefinitions")
    private List<IndexKeyDefinition> idxDefs;

    /** Map of index definitions with proper order. */
    private LinkedHashMap<String, IndexKeyDefinition> idxDefsMap;

    /** */
    public IndexQueryResultMeta() {
        // No-op.
    }

    /** */
    public IndexQueryResultMeta(SortedIndexDefinition def, int critSize) {
        keyTypeSettings = def.keyTypeSettings();

        idxDefsMap = U.newLinkedHashMap(critSize);

        Iterator<Map.Entry<String, IndexKeyDefinition>> keys = def.indexKeyDefinitions().entrySet().iterator();

        for (int i = 0; i < critSize; i++) {
            Map.Entry<String, IndexKeyDefinition> key = keys.next();

            idxDefsMap.put(key.getKey(), key.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 18;
    }

    /** */
    public IndexKeyTypeSettings keyTypeSettings() {
        return keyTypeSettings;
    }

    /** */
    public void keyTypeSettings(IndexKeyTypeSettings keyTypeSettings) {
        this.keyTypeSettings = keyTypeSettings;
    }

    /** @return Map of index definitions with proper order. */
    public LinkedHashMap<String, IndexKeyDefinition> keyDefinitions() {
        return idxDefsMap;
    }

    /** @return Index names with proper order. */
    public Collection<String> orderredIndexNames() {
        assert idxDefsMap != null;

        return idxDefsMap.keySet();
    }

    /**
     * Stores index names with proper order to build the linked map later.
     * Should be called once and before the setting of the definitions and the map.
     */
    public void orderredIndexNames(List<String> idxNames) {
        assert idxNames != null : "Index names cannot be null.";
        assert this.idxNames == null : "Index names should be set once.";
        assert idxDefs == null : "Index definitions should not be initialized yet.";
        assert idxDefsMap == null : "Index definitions map should not be initialized yet.";

        this.idxNames = idxNames;
    }

    /** @return Index definitions with proper order. */
    public Collection<IndexKeyDefinition> orderredIndexDefinitions() {
        assert idxDefsMap != null;

        return idxDefsMap.values();
    }

    /**
     * Process the index definitions with proper order and buils the linked map.
     * Should be called once and after the setting of the index names.
     */
    public void orderredIndexDefinitions(List<IndexKeyDefinition> idxDefs) {
        assert idxDefs != null : "Index definitions cannot be null.";
        assert idxNames != null && idxNames.size() == idxDefs.size() : "Index names should be already properly initialized.";
        assert idxDefsMap == null : "Index definitions map should not be initialized yet.";

        idxDefsMap = U.newLinkedHashMap(idxDefs.size());

        for (int i = 0; i < idxDefs.size(); i++)
            idxDefsMap.put(idxNames.get(i), idxDefs.get(i));

        idxNames = null;
    }
}
