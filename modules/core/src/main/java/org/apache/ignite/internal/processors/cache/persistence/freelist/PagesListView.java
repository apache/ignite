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

package org.apache.ignite.internal.processors.cache.persistence.freelist;

import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.spi.systemview.view.SystemView;

/**
 * Pages-list representation for a {@link SystemView}.
 */
public class PagesListView {
    /** Pages list. */
    PagesList pagesList;

    /** Bucket number. */
    int bucket;

    /**
     * @param pagesList Pages list.
     * @param bucket Bucket number.
     */
    public PagesListView(PagesList pagesList, int bucket) {
        this.pagesList = pagesList;
        this.bucket = bucket;
    }

    /**
     * @return Cache group id.
     */
    @Order
    public int cacheGroupId() {
        return pagesList.groupId();
    }

    /**
     * @return Pages-list name.
     */
    @Order(1)
    public String name() {
        return pagesList.name;
    }

    /**
     * @return Bucket number.
     * */
    @Order(2)
    public int bucketNumber() {
        return bucket;
    }

    /**
     * @return Bucket size.
     */
    @Order(3)
    public long bucketSize() {
        return pagesList.bucketsSize[bucket].get();
    }

    /**
     * @return Bucket stripes count.
     */
    @Order(4)
    public int stripesCount() {
        PagesList.Stripe[] stripes = pagesList.getBucket(bucket);

        return stripes == null ? 0 : stripes.length;
    }

    /**
     * @return Count of pages cached onheap.
     */
    @Order(5)
    public int cachedPagesCount() {
        PagesList.PagesCache pagesCache = pagesList.getBucketCache(bucket, false);

        return pagesCache == null ? 0 : pagesCache.size();
    }
}
