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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import org.apache.ignite.client.ClientException;

/**
 * Extracts paged data
 */
interface QueryPager<T> extends AutoCloseable {
    /**
     * Reads next page. Call {@link this#hasNext()} to check if there is data to read before calling this method.
     */
    public Collection<T> next() throws ClientException;

    /**
     * @return {@code true} if there are more pages to read; {@code false} otherwise.
     */
    public boolean hasNext();

    /** Indicates if initial query response was received. */
    public boolean hasFirstPage();

    /**
     * Reset query pager.
     */
    public void reset();
}
