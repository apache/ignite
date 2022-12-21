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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import org.apache.ignite.internal.cache.query.index.IndexDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.SortedSegmentedIndex;

/**
 * Represents an index that inlines some of index keys.
 */
public interface InlineIndex extends SortedSegmentedIndex {
    /**
     * @return amount of bytes to store inlined index keys.
     */
    public int inlineSize();

    /**
     * {@code true} if index is created and {@code false} if it is restored from disk.
     */
    public boolean created();

    /**
     * @param segment Number of tree segment.
     * @return Tree segment for specified number.
     */
    public InlineIndexTree segment(int segment);

    /**
     * @return Index definition.
     */
    public IndexDefinition indexDefinition();
}
