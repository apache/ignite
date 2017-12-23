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

import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;

/**
 * Class containing key and remote context (see explanation of remote context in {@link ComputationsChain}).
 *
 * @param <K> Cache key type.
 * @param <C> Remote context.
 */
public class KeyAndContext<K, C> {
    /**
     * Key of group trainer.
     */
    private GroupTrainerCacheKey<K> key;

    /**
     * Remote context.
     */
    private C context;

    /**
     * Construct instance of this class.
     *
     * @param key Cache key.
     * @param context Remote context.
     */
    public KeyAndContext(GroupTrainerCacheKey<K> key, C context) {
        this.key = key;
        this.context = context;
    }

    /**
     * Get group trainer cache key.
     *
     * @return Group trainer cache key.
     */
    public GroupTrainerCacheKey<K> key() {
        return key;
    }

    /**
     * Get remote context.
     *
     * @return Remote context.
     */
    public C context() {
        return context;
    }
}
