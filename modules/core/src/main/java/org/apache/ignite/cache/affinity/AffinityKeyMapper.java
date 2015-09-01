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

package org.apache.ignite.cache.affinity;

import java.io.Serializable;

/**
 * Affinity mapper which maps cache key to an affinity key. Affinity key is a key which will be
 * used to determine a node on which this key will be cached. Every cache key will first be passed
 * through {@link #affinityKey(Object)} method, and the returned value of this method
 * will be given to {@link AffinityFunction} implementation to find out key-to-node affinity.
 * <p>
 * The default implementation, which will be used if no explicit affinity mapper is specified
 * in cache configuration, will first look for any field or method annotated with
 * {@link AffinityKeyMapped @AffinityKeyMapped} annotation. If such field or method
 * is not found, then the cache key itself will be returned from {@link #affinityKey(Object) affinityKey(Object)}
 * method (this means that all objects with the same cache key will always be routed to the same node).
 * If such field or method is found, then the value of this field or method will be returned from
 * {@link #affinityKey(Object) affinityKey(Object)} method. This allows to specify alternate affinity key, other
 * than the cache key itself, whenever needed.
 * <p>
 * A custom (other than default) affinity mapper can be provided
 * via {@link org.apache.ignite.configuration.CacheConfiguration#getAffinityMapper()} configuration property.
 * <p>
 * For more information on affinity mapping and examples refer to {@link AffinityFunction} and
 * {@link AffinityKeyMapped @AffinityKeyMapped} documentation.
 * @see AffinityFunction
 * @see AffinityKeyMapped
 */
public interface AffinityKeyMapper extends Serializable {
    /**
     * Maps passed in key to an alternate key which will be used for node affinity.
     *
     * @param key Key to map.
     * @return Key to be used for node-to-affinity mapping (may be the same
     *      key as passed in).
     */
    public Object affinityKey(Object key);

    /**
     * Resets cache affinity mapper to its initial state. This method will be called by
     * the system any time the affinity mapper has been sent to remote node where
     * it has to be reinitialized. If your implementation of affinity mapper
     * has no initialization logic, leave this method empty.
     */
    public void reset();
}