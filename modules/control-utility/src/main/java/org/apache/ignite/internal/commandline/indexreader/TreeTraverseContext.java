/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.commandline.indexreader;

import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;

import java.util.List;
import java.util.Map;

import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.getCacheId;

/**
 * Traverse context, which is used for tree traversal and is unique for traversal of one single tree.
 */
class TreeTraverseContext {
    /** Tree name. */
    final String treeName;

    /** Cache id. */
    final int cacheId;

    /** Page store. */
    final FilePageStore store;

    /** Page type statistics. */
    final Map<Class, Long> ioStat;

    /** Map of errors, pageId -> set of exceptions. */
    final Map<Long, List<Throwable>> errors;

    /** Callback that is called for each inner node page. */
    final PageCallback innerCb;

    /** Callback that is called for each leaf node page.*/
    final PageCallback leafCb;

    /** Callback that is called for each leaf item. */
    final ItemCallback itemCb;

    /** */
    public TreeTraverseContext(
            String treeName,
            FilePageStore store,
            Map<Class, Long> ioStat,
            Map<Long, List<Throwable>> errors,
            PageCallback innerCb,
            PageCallback leafCb,
            ItemCallback itemCb
    ) {
        this.treeName = treeName;
        this.cacheId = getCacheId(treeName);
        this.store = store;
        this.ioStat = ioStat;
        this.errors = errors;
        this.innerCb = innerCb;
        this.leafCb = leafCb;
        this.itemCb = itemCb;
    }
}