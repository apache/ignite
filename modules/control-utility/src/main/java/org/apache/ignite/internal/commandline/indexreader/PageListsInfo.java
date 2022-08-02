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

import java.util.List;
import java.util.Map;

import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 *
 */
class PageListsInfo {
    /**
     * Page list bucket data (next meta id, bucket index) -> list of page ids.
     * See {@link PagesListMetaIO#getBucketsData }.
     */
    final Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData;

    /** Found pages count. */
    final long pagesCnt;

    /** Pages statistics. */
    final Map<Class<? extends PageIO>, ScanContext.PagesStatistic> stats;

    /** Errors count. */
    final long errCnt;

    /** */
    public PageListsInfo(
        Map<IgniteBiTuple<Long, Integer>, List<Long>> bucketsData,
        long pagesCnt,
        Map<Class<? extends PageIO>, ScanContext.PagesStatistic> stats,
        long errCnt
    ) {
        this.bucketsData = bucketsData;
        this.pagesCnt = pagesCnt;
        this.stats = stats;
        this.errCnt = errCnt;
    }
}
