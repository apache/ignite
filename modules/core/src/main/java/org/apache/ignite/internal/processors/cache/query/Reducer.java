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

package org.apache.ignite.internal.processors.cache.query;

import org.apache.ignite.IgniteCheckedException;

/**
 * This class is responsible for iterating over cache query results. Custom reduce logic is applied within {@link #next()}
 * and {@link #hasNext()} methods.
 */
public interface Reducer<T> {
    /**
     * @return Next item.
     */
    public T next() throws IgniteCheckedException;

    /**
     * @return {@code true} if there is a next item, otherwise {@code false}.
     */
    public boolean hasNext() throws IgniteCheckedException;

    /**
     * Lock object that is shared between {@link GridCacheQueryFutureAdapter} and Reducer.
     */
    public Object sharedLock();

    /**
     * Offer query result page for reduce.
     *
     * @param page Page.
     */
    public void addPage(CacheQueryResultPage<T> page);

    /**
     * Callback that invokes after reducer get last query result page.
     * Also invokes for failed queries to let reducer know that there won't be new pages.
     */
    public void onLastPage();
}
