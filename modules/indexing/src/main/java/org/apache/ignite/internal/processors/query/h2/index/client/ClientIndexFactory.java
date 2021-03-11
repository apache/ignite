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

package org.apache.ignite.internal.processors.query.h2.index.client;

import java.util.List;
import org.apache.ignite.internal.cache.query.index.Index;
import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.IndexFactory;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexTree;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.query.h2.index.QueryIndexKeyDefinitionProvider;

/**
 * Factory fot client index.
 */
public class ClientIndexFactory implements IndexFactory {
    /** Instance. */
    public static final ClientIndexFactory INSTANCE = new ClientIndexFactory();

    /** Dummy key types. */
    private static final IndexKeyTypeSettings DUMMY_SETTINGS = new IndexKeyTypeSettings();

    /** Forbidden constructor. */
    private ClientIndexFactory() {}

    /** {@inheritDoc} */
    @Override public Index createIndex(GridCacheContext<?, ?> cctx, IndexDefinition definition) {
        ClientIndexDefinition def = (ClientIndexDefinition) definition;

        List<IndexKeyDefinition> keyDefs = new QueryIndexKeyDefinitionProvider(def.getTable(), def.getColumns()).keyDefinitions();

        List<InlineIndexKeyType> keyTypes = InlineIndexKeyTypeRegistry.types(keyDefs, DUMMY_SETTINGS);

        int inlineSize = InlineIndexTree.computeInlineSize(keyTypes, def.getCfgInlineSize(), def.getMaxInlineSize());

        return new ClientInlineIndex(def.idxName().idxName(), inlineSize);
    }
}
