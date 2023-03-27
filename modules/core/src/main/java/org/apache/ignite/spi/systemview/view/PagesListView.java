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

package org.apache.ignite.spi.systemview.view;

import org.apache.ignite.internal.managers.systemview.walker.Filtrable;
import org.apache.ignite.internal.managers.systemview.walker.Order;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;

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
     * @return Pages-list name.
     */
    @Order(2)
    public String name() {
        return pagesList.name();
    }

    /**
     * @return Bucket number.
     * */
    @Order(3)
    @Filtrable
    public int bucketNumber() {
        return bucket;
    }

    /**
     * @return Bucket size.
     */
    @Order(4)
    public long bucketSize() {
        return pagesList.bucketSize(bucket);
    }

    /**
     * @return Bucket stripes count.
     */
    @Order(5)
    public int stripesCount() {
        return pagesList.stripesCount(bucket);
    }

    /**
     * @return Count of pages cached onheap.
     */
    @Order(6)
    public int cachedPagesCount() {
        return pagesList.cachedPagesCount(bucket);
    }
}
