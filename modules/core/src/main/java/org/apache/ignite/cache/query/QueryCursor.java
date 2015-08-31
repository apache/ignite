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

package org.apache.ignite.cache.query;

import java.util.List;

/**
 * Query result cursor. Implements {@link Iterable} only for convenience, e.g. {@link #iterator()}
 * can be obtained only once. Also if iteration is started then {@link #getAll()} method calls are prohibited.
 * <p>
 * Not thread safe and must be used from single thread only.
 */
public interface QueryCursor<T> extends Iterable<T>, AutoCloseable {
    /**
     * Gets all query results and stores them in the collection.
     * Use this method when you know in advance that query result is
     * relatively small and will not cause memory utilization issues.
     * <p>
     * Since all the results will be fetched, all the resources will be closed
     * automatically after this call, e.g. there is no need to call {@link #close()} method in this case.
     *
     * @return List containing all query results.
     */
    public List<T> getAll();

    /**
     * Closes all resources related to this cursor.
     */
    @Override public void close();
}