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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.Map;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexPlainRowImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.InlineIndexRowHandler;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndex;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKeyFactory;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.exec.exp.RangeIterable;
import org.apache.ignite.internal.processors.query.calcite.schema.CacheTableDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

/** Extension for column {@value QueryUtils#KEY_FIELD_NAME} in case of composite primary key. */
public class IndexWrappedKeyScan<Row> extends IndexScan<Row> {
    /** */
    public IndexWrappedKeyScan(
        ExecutionContext<Row> ectx,
        CacheTableDescriptor desc,
        InlineIndex idx,
        ImmutableIntList idxFieldMapping,
        int[] parts,
        RangeIterable<Row> ranges,
        @Nullable ImmutableBitSet requiredColumns
    ) {
        super(ectx, desc, idx, idxFieldMapping, parts, ranges, requiredColumns);
    }

    /** */
    @Override protected IndexRow row2indexRow(Row bound) {
        if (bound == null)
            return null;

        RowHandler<Row> rowHnd = ectx.rowHandler();

        Object key = rowHnd.get(QueryUtils.KEY_COL, bound);
        assert key != null : String.format("idxName=%s, bound=%s", idx.name(), Commons.toString(rowHnd, bound));

        if (key instanceof BinaryObject)
            return binaryObject2indexRow((BinaryObject)key);

        throw new IgniteException(String.format(
            "Unsupported type for index boundary: [expected=%s, current=%s]",
            BinaryObject.class.getName(), key.getClass().getName()
        ));
    }

    /** */
    private IndexRow binaryObject2indexRow(BinaryObject o) {
        assert o.type().typeName().equals(idx.indexDefinition().typeDescriptor().keyTypeName()) : String.format(
            "idx=%s, o=%s, oType=%s, idxKeyType=%s",
            idx.name(), o, o.type().typeName(), idx.indexDefinition().typeDescriptor().keyTypeName()
        );

        InlineIndexRowHandler idxRowHnd = idx.segment(0).rowHandler();
        IndexKey[] keys = new IndexKey[idx.indexDefinition().indexKeyDefinitions().size()];

        int i = 0;
        for (Map.Entry<String, IndexKeyDefinition> e : idx.indexDefinition().indexKeyDefinitions().entrySet()) {
            String keyName = e.getKey();

            ColumnDescriptor fieldDesc = desc.columnDescriptor(keyName);
            assert fieldDesc != null : String.format("idx=%s, o=%s, keyName=%s", idx.name(), o, keyName);

            Object field = o.field(keyName);
            Object key = TypeUtils.fromInternal(ectx, field, fieldDesc.storageType());

            keys[i++] = wrapIndexKey(key, e.getValue().indexKeyType());
        }

        return new IndexPlainRowImpl(keys, idxRowHnd);
    }

    /** */
    private IndexKey wrapIndexKey(Object key, IndexKeyType keyType) {
        return IndexKeyFactory.wrap(key, keyType, cctx.cacheObjectContext(), idx.indexDefinition().keyTypeSettings());
    }
}
