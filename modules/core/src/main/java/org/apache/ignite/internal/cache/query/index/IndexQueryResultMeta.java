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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.MetaPageInfo;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Metadata for IndexQuery response. This information is required to be sent to a node that initiated a query.
 * Thick client nodes may have irrelevant information about index structure, {@link MetaPageInfo}.
 */
public class IndexQueryResultMeta implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Index key settings. */
    private IndexKeyTypeSettings keyTypeSettings;

    /** Index key definitions. */
    private LinkedHashMap<String, IndexKeyDefinition> keyDefs;

    /** */
    public IndexQueryResultMeta() {
        // No-op.
    }

    /** */
    public IndexQueryResultMeta(SortedIndexDefinition def, int critSize) {
        keyTypeSettings = def.keyTypeSettings();

        keyDefs = new LinkedHashMap<>();

        Iterator<Map.Entry<String, IndexKeyDefinition>> keys = def.indexKeyDefinitions().entrySet().iterator();

        for (int i = 0; i < critSize; i++) {
            Map.Entry<String, IndexKeyDefinition> key = keys.next();

            keyDefs.put(key.getKey(), key.getValue());
        }
    }

    /** */
    public IndexKeyTypeSettings keyTypeSettings() {
        return keyTypeSettings;
    }

    /** */
    public LinkedHashMap<String, IndexKeyDefinition> keyDefinitions() {
        return keyDefs;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(keyTypeSettings);

        U.writeMap(out, keyDefs);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        keyTypeSettings = (IndexKeyTypeSettings)in.readObject();

        keyDefs = U.readLinkedMap(in);
    }
}
