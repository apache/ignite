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

import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.normalizePageId;

/**
 * Traverse context, which is used for tree traversal and is unique for traversal of one single tree.
 */
class TreeTraverseContext {
    /** Root page id. */
    final long rootPageId;

    /** Tree name. */
    final String treeName;

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

    /** */
    public TreeTraverseContext(
        long rootPageId,
        String treeName,
        FilePageStore store,
        ItemStorage itemStorage,
        Set<Long> innerPageIds
    ) {
        this.rootPageId = rootPageId;
        this.treeName = treeName;
        this.store = store;
        this.itemStorage = itemStorage;
        this.ioStat = new HashMap<>();
        this.errors = new HashMap<>();
        this.innerPageIds = innerPageIds;
    }

    /** */
    public void onPageIO(PageIO io) {
        ioStat.compute(io.getClass(), (k, v) -> v == null ? 1 : v + 1);
    }

    /** */
    public void onInnerPage(long pageId) {
        innerPageIds.add(normalizePageId(pageId));
    }

    /** */
    public void onLeafPage(long pageId, List<Object> data) {
        data.forEach(itemStorage::add);
    }
}
