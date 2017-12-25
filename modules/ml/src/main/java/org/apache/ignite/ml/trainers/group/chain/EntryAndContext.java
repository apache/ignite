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

package org.apache.ignite.ml.trainers.group.chain;

import java.util.Map;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;

/**
 * Entry of cache used for group training and context.
 * This class is used as input for workers of distributed steps of {@link ComputationsChain}.
 *
 * @param <K> Type of cache keys used for training.
 * @param <V> Type of cache values used for training.
 * @param <C> Type of context.
 */
public class EntryAndContext<K, V, C> {
    /**
     * Entry of cache used for training.
     */
    private Map.Entry<GroupTrainerCacheKey<K>, V> entry;

    /**
     * Context.
     */
    private C ctx;

    /**
     * Construct instance of this class.
     *
     * @param entry Entry of cache used for training.
     * @param ctx Context.
     */
    public EntryAndContext(Map.Entry<GroupTrainerCacheKey<K>, V> entry, C ctx) {
        this.entry = entry;
        this.ctx = ctx;
    }

    /**
     * Get entry of cache used for training.
     *
     * @return Entry of cache used for training.
     */
    public Map.Entry<GroupTrainerCacheKey<K>, V> entry() {
        return entry;
    }

    /**
     * Get context.
     *
     * @return Context.
     */
    public C context() {
        return ctx;
    }
}
