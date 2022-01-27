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
    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, IndexKey key, int curType) {
        if (curType == IndexKeyTypes.UNKNOWN)
            return CANT_BE_COMPARE;

        if (key == NullIndexKey.INSTANCE)
            return 1;

        // Check that types are different before that.
        return COMPARE_UNSUPPORTED;
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
    private int compare(IndexKey lkey, IndexKey rkey) {
        if (lkey == NullIndexKey.INSTANCE)
            return lkey.compare(rkey);
        else if (rkey == NullIndexKey.INSTANCE)
            return 1;

        if (lkey.type() == rkey.type())
            return lkey.compare(rkey);

        return COMPARE_UNSUPPORTED;
    }
}
