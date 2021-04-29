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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class is used to store result of tree traversal.
 */
class TreeTraversalInfo {
    /** Page type statistics. */
    final Map<Class, Long> ioStat;

    /** Map of errors, pageId -> set of exceptions. */
    final Map<Long, List<Throwable>> errors;

    /** Set of all inner page ids. */
    final Set<Long> innerPageIds;

    /** Root page id. */
    final long rootPageId;

    /**
     * List of items storage.
     */
    final ItemStorage itemStorage;

    /** */
    public TreeTraversalInfo(
            Map<Class, Long> ioStat,
            Map<Long, List<Throwable>> errors,
            Set<Long> innerPageIds,
            long rootPageId,
            ItemStorage itemStorage
    ) {
        this.ioStat = ioStat;
        this.errors = errors;
        this.innerPageIds = innerPageIds;
        this.rootPageId = rootPageId;
        this.itemStorage = itemStorage;
    }
}