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

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;

import static org.apache.ignite.internal.commandline.indexreader.IgniteIndexReader.ERROR_PREFIX;

/**
 * Traverse context, which is used for tree traversal and is unique for traversal of one single tree.
 */
class ScanContext {
    /** Cache id or {@code -1} for sequential scan. */
    final int cacheId;

    /** Count of inline fields. */
    final int inlineFldCnt;

    /** Page store. */
    final FilePageStore store;

    /** Page type statistics. */
    final Map<Class<? extends PageIO>, PagesStatistic> stats;

    /** Map of errors, pageId -> set of exceptions. */
    final Map<Long, List<String>> errors;

    long errCnt;

    /** List of items storage. */
    final ItemStorage items;

    /**
     * Inline usage statistics.
     * Size of the array equal index inline size.
     * Each cell contains count of item that use exact number of inline bytes.
     */
    int[] inline;

    /** */
    private final Logger log;

    /** */
    private final String prefix;

    /** */
    public ScanContext(int cacheId, int inlineFldCnt, FilePageStore store, ItemStorage items, Logger log, String prefix) {
        this.cacheId = cacheId;
        this.inlineFldCnt = inlineFldCnt;
        this.store = store;
        this.items = items;
        this.stats = new LinkedHashMap<>();
        this.errors = new LinkedHashMap<>();
        this.log = log;
        this.prefix = prefix;
    }

    /** */
    public void addToStats(PageIO io, long addr) {
        addToStats(io, stats, 1, addr, store.getPageSize());
    }

    /** */
    public static void addToStats(PageIO io, Map<Class<? extends PageIO>, PagesStatistic> stats, long cnt, long addr, int pageSize) {
        PagesStatistic stat = stats.computeIfAbsent(io.getClass(), k -> new PagesStatistic());

        stat.cnt += cnt;
        stat.freeSpace += io.getFreeSpace(pageSize, addr);
    }

    /** */
    public static void addToStats(
        Class<? extends PageIO> io,
        Map<Class<? extends PageIO>, PagesStatistic> stats,
        PagesStatistic toAdd
    ) {
        PagesStatistic stat = stats.computeIfAbsent(io, k -> new PagesStatistic());

        stat.cnt += toAdd.cnt;
        stat.freeSpace += toAdd.freeSpace;
    }

    /** */
    public void onLeafPage(long pageId, List<Object> data) {
        data.forEach(items::add);
    }

    /** */
    public void onError(long pageId, String message) {
        errors.computeIfAbsent(pageId, k -> new LinkedList<>()).add(message);

        if (errCnt == 0)
            log.warning(prefix + ERROR_PREFIX + "---- Errors:");

        errCnt++;

        onError(log, prefix, pageId, message);
    }

    public static void onError(Logger log, String prefix, long pageId, String message) {
        log.warning(prefix + ERROR_PREFIX + "Page id: " + pageId + ", exceptions: " + message);
    }

    /** */
    static class PagesStatistic {
        /** Count of pages. */
        long cnt;

        /** Summary free space. */
        long freeSpace;
    }
}
