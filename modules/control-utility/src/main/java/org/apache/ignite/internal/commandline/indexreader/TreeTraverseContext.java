/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.indexreader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongConsumer;

import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.jetbrains.annotations.Nullable;

/**
 * Traverse context, which is used for tree traversal and is unique for traversal of one single tree.
 */
class TreeTraverseContext {
    /** Root page id. */
    final long rootPageId;

    /** Tree name. */
    final String treeName;

    /** Cache id. */
    final int cacheId;

    /** Page store. */
    final FilePageStore store;

    /** Page type statistics. */
    final Map<Class<? extends PageIO>, Long> ioStat;

    /** Map of errors, pageId -> set of exceptions. */
    final Map<Long, List<Throwable>> errors;

    /** List of items storage. */
    final ItemStorage itemStorage;

    /** Set of all inner page ids. */
    final Set<Long> innerPageIds;

    /** Callback that is called for each inner node page. */
    @Nullable final LongConsumer innerCb;

    /** Callback that is called for each leaf node page.*/
    @Nullable final LongConsumer leafCb;

    /** */
    public TreeTraverseContext(
        long rootPageId,
        String treeName,
        FilePageStore store,
        ItemStorage itemStorage,
        Set<Long> innerPageIds,
        @Nullable LongConsumer innerCb,
        @Nullable LongConsumer leafCb
    ) {
        this.rootPageId = rootPageId;
        this.treeName = treeName;
        this.cacheId = IgniteIndexReader.getCacheAndTypeId(treeName).get1();
        this.store = store;
        this.itemStorage = itemStorage;
        this.ioStat = new HashMap<>();
        this.errors = new HashMap<>();
        this.innerPageIds = innerPageIds;
        this.innerCb = innerCb;
        this.leafCb = leafCb;
    }
}
