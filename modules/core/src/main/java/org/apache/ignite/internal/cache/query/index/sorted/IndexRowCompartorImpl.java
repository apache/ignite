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
 *
 */

package org.apache.ignite.internal.cache.query.index.sorted;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;

import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.CANT_BE_COMPARE;
import static org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType.COMPARE_UNSUPPORTED;

/**
 * Provide default logic of rows comparison.
 *
 * Consider:
 * 1. NULL is the least value.
 * 2. Comparison of different types is not supported.
 */
public class IndexRowCompartorImpl implements IndexRowComparator {
    /** Key type settings for this index. */
    protected final IndexKeyTypeSettings keyTypeSettings;

    /** */
    public IndexRowCompartorImpl(IndexKeyTypeSettings settings) {
        keyTypeSettings = settings;
    }

    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, IndexKey key, InlineIndexKeyType type) {
        if (type.type() == IndexKeyType.UNKNOWN)
            return CANT_BE_COMPARE;

        // Value can be set up by user in query with different data type. Don't compare uncomparable types in this comparator.
        if (isInlineComparable(type, key)) {
            // If inlining of POJO is not supported then don't compare it here.
            if (type.type() != IndexKeyType.JAVA_OBJECT || keyTypeSettings.inlineObjSupported())
                return type.compare(pageAddr, off, maxSize, key);
            else
                return CANT_BE_COMPARE;
        }

        return COMPARE_UNSUPPORTED;
    }

    /** */
    private boolean isInlineComparable(InlineIndexKeyType type, IndexKey key) {
        if (key == NullIndexKey.INSTANCE)
            return true;

        return type.isComparableTo(key);
    }

    /** {@inheritDoc} */
    @Override public int compareRow(IndexRow left, IndexRow right, int idx) throws IgniteCheckedException {
        return compare(left.key(idx), right.key(idx));
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexKey left, IndexKey right) throws IgniteCheckedException {
        if (left == right)
            return 0;

        return compare(left, right);
    }

    /** */
    private int compare(IndexKey lkey, IndexKey rkey) throws IgniteCheckedException {
        try {
            if (lkey == NullIndexKey.INSTANCE)
                return lkey.compare(rkey);
            else if (rkey == NullIndexKey.INSTANCE)
                return 1;

            if (lkey.isComparableTo(rkey))
                return lkey.compare(rkey);
            else if (rkey.isComparableTo(lkey))
                return -rkey.compare(lkey);
        }
        catch (RuntimeException e) {
            // Runtime exceptions should be wrapped into checked exception, since any runtime exception treated by
            // B+tree as corrupted tree exception and triggers failure handler.
            throw new IgniteCheckedException(e);
        }

        throw new IgniteCheckedException("Values can't be compared [lkey=" + lkey + ", rkey=" + rkey + ']');
    }
}
