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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.NullableInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;

/**
 * Comparator for index rows.
 */
public interface IndexRowComparator {
    /**
     * Compare inlined index key with specified key. If it is impossible to compare inlined key, it returns special vals:
     * {@code COMPARE_UNSUPPORTED} and {@code CANT_BE_COMPARE}, see {@link NullableInlineIndexKeyType}.
     *
     * @param pageAddr address of an index row.
     * @param off offset of an index key.
     * @param maxSize max size to read.
     * @param key key to compare with.
     * @param type inline type of index key.
     */
    public int compareKey(long pageAddr, int off, int maxSize, IndexKey key, InlineIndexKeyType type) throws IgniteCheckedException;

    /**
     * Compare index keys.
     *
     * @param left index key.
     * @param right index key.
     */
    public int compareKey(IndexKey left, IndexKey right) throws IgniteCheckedException;

    /**
     * Compare index rows by key specified with {@code idx}.
     *
     * @param left index row.
     * @param right index row.
     * @param idx offset of index key.
     */
    public int compareRow(IndexRow left, IndexRow right, int idx) throws IgniteCheckedException;
}
