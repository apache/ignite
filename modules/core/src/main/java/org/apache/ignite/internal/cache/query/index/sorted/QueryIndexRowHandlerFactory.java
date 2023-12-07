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

package org.apache.ignite.internal.cache.query.index.sorted;

import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;

/**
 * This factory applies tree's meta page info to build info about inlined types.
 */
public class QueryIndexRowHandlerFactory implements InlineIndexRowHandlerFactory {
    /** {@inheritDoc} */
    @Override public InlineIndexRowHandler create(SortedIndexDefinition sdef, IndexKeyTypeSettings keyTypeSettings) {
        LinkedHashMap<String, IndexKeyDefinition> keyDefs = sdef.indexKeyDefinitions();

        List<InlineIndexKeyType> keyTypes = InlineIndexKeyTypeRegistry.types(keyDefs.values(), keyTypeSettings);

        return new QueryIndexRowHandler(sdef.typeDescriptor(), sdef.cacheInfo(), keyDefs, keyTypes, keyTypeSettings);
    }
}
