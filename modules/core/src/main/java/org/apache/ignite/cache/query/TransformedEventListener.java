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

/**
 * Interface for local listener of {@link ContinuousQueryWithTransformer} to implement.
 * Invoked if an cache entry is updated, created or if a batch call is made,
 * after the entries are updated and transformed.
 *
 * @param <T> type of data produced by transformer {@link ContinuousQueryWithTransformer#getRemoteTransformerFactory()}.
 * @see ContinuousQueryWithTransformer
 * @see ContinuousQueryWithTransformer#setLocalListener(TransformedEventListener)
 */
public interface TransformedEventListener<T> {
    /**
     * Called after one or more entries have been updated.
     *
     * @param events The entries just updated that transformed with remote transformer of {@link ContinuousQueryWithTransformer}.
     */
    void onUpdated(Iterable<? extends T> events);
}
