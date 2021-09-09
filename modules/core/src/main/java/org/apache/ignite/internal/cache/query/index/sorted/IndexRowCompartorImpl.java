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

import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;

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

    /** Index key definitions. Sets after initialization of comparator. */
    protected List<IndexKeyDefinition> keyDefs;

    /** */
    public IndexRowCompartorImpl(IndexKeyTypeSettings keyTypeSettings) {
        this.keyTypeSettings = keyTypeSettings;
    }

    /** {@inheritDoc} */
    @Override public int compareKey(long pageAddr, int off, int maxSize, IndexKey key, int curType, int keyIdx) {
        if (curType == IndexKeyTypes.UNKNOWN)
            return CANT_BE_COMPARE;

        return COMPARE_UNSUPPORTED;
    }

    /** {@inheritDoc} */
    @Override public int compareKey(IndexRow left, IndexRow right, int idx) throws IgniteCheckedException {
        IndexKey lkey = left.key(idx);
        IndexKey rkey = right.key(idx);

        if (lkey.type() == IndexKeyTypes.NULL && rkey.type() == IndexKeyTypes.NULL)
            return 0;
        else if (lkey.type() == IndexKeyTypes.NULL)
            return keyDefs.get(idx).order().compareWithNull(true);
        else if (rkey.type() == IndexKeyTypes.NULL)
            return keyDefs.get(idx).order().compareWithNull(false);

        if (lkey.type() == rkey.type())
            return lkey.compare(rkey);

        return COMPARE_UNSUPPORTED;
    }

    /** */
    public void setKeyDefinitions(List<IndexKeyDefinition> keyDefs) {
        this.keyDefs = keyDefs;
    }
}
