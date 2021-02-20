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

package org.apache.ignite.internal.processors.query.h2.index;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandlerFactory;
import org.apache.ignite.internal.cache.query.index.sorted.SortedIndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyTypeRegistry;
import org.h2.table.IndexColumn;

/**
 * This factory applies tree's meta page info to build info about inlined types.
 */
public class QueryRowHandlerFactory implements InlineIndexRowHandlerFactory {
    /** {@inheritDoc} */
    @Override public InlineIndexRowHandler create(SortedIndexDefinition sdef, Object... args) {
        boolean useUnwrappedPk = (boolean) args[0];
        boolean inlineObjHash = (boolean) args[1];

        QueryIndexDefinition def = (QueryIndexDefinition) sdef;
        def.setUpFlags(useUnwrappedPk, inlineObjHash);

        List<IndexKeyDefinition> keyDefs = def.getIndexKeyDefinitions();

        List<IndexColumn> h2IdxColumns = useUnwrappedPk ? def.h2UnwrappedCols : def.h2WrappedCols;

        List<InlineIndexKeyType> keyTypes = new ArrayList<>();

        for (IndexKeyDefinition keyDef: keyDefs) {
            if (!InlineIndexKeyTypeRegistry.supportInline(keyDef.getIdxType()))
                break;

            keyTypes.add(
                InlineIndexKeyTypeRegistry.get(keyDef.getIdxClass(), keyDef.getIdxType(), !inlineObjHash));
        }

        return new QueryIndexRowHandler(def.getTable(), h2IdxColumns, keyDefs, keyTypes);
    }
}
