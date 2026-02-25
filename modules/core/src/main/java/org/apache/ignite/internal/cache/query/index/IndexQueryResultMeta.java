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

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.Order;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jetbrains.annotations.Nullable;

/**
 * Metadata for IndexQuery response. This information is required to be sent to a node that initiated a query.
 * Thick client nodes may have irrelevant information about index structure, {@link MetaPageInfo}.
 */
public class IndexQueryResultMeta implements Message {
    /** Index key settings. */
    @Order(0)
    IndexKeyTypeSettings keyTypeSettings;

    /** Index names order holder. Should be serialized together with  the definitions. */
    @Order(1)
    @Nullable String[] idxNames;

    /** Index definitions serialization holder. Should be serialized together with the names. */
    @Order(2)
    @Nullable IndexKeyDefinition[] idxDefs;

    /** */
    public IndexQueryResultMeta() {
        // No-op.
    }

    /** */
    public IndexQueryResultMeta(SortedIndexDefinition def, int critSize) {
        keyTypeSettings = def.keyTypeSettings();

        Iterator<Map.Entry<String, IndexKeyDefinition>> keys = def.indexKeyDefinitions().entrySet().iterator();

        if (critSize > 0) {
            idxNames = new String[critSize];
            idxDefs = new IndexKeyDefinition[critSize];

            for (int i = 0; i < critSize; i++) {
                Map.Entry<String, IndexKeyDefinition> key = keys.next();

                idxNames[i] = key.getKey();
                idxDefs[i] = key.getValue();
            }
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

    /** @return Map of index definitions with proper order. */
    public LinkedHashMap<String, IndexKeyDefinition> keyDefinitions() {
        if (F.isEmpty(idxNames) && F.isEmpty(idxDefs))
            return U.newLinkedHashMap(0);

        assert idxNames.length == idxDefs.length : "Number of index names and index definitions must be equal " +
            "[idxNames=" + Arrays.toString(idxNames) + ", idxDefs=" + Arrays.toString(idxDefs) + "]";

        LinkedHashMap<String, IndexKeyDefinition> idxDefsMap = U.newLinkedHashMap(idxNames.length);

        for (int i = 0; i < idxNames.length; i++)
            idxDefsMap.put(idxNames[i], idxDefs[i]);

        return idxDefsMap;
    }

}
