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

import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
class PageListsInfo {
    /**
     * Page list bucket data (next meta id, bucket index) -> list of page ids.
     * See {@link PagesListMetaIO#getBucketsData }.
     */
    final Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData;

    /** All page ids from page lists. */
    final Set<Long> allPages;

    /** Page type statistics. */
    final Map<Class, Long> pageListStat;

    /** Map of errors, pageId -> list of exceptions. */
    final Map<Long, List<Throwable>> errors;

    /** */
    public PageListsInfo(
            Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData,
            Set<Long> allPages,
            Map<Class, Long> pageListStat,
            Map<Long, List<Throwable>> errors
    ) {
        this.bucketsData = bucketsData;
        this.allPages = allPages;
        this.pageListStat = pageListStat;
        this.errors = errors;
    }
}