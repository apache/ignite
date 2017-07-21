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

package org.apache.ignite.internal.processors.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;

/**
 * IGFS utility processor adapter.
 */
public interface IgfsHelper {
    /**
     * Pre-process cache configuration.
     *
     * @param cfg Cache configuration.
     */
    public abstract void preProcessCacheConfiguration(CacheConfiguration cfg);

    /**
     * Validate cache configuration for IGFS.
     *
     * @param cfg Cache configuration.
     * @throws IgniteCheckedException If validation failed.
     */
    public abstract void validateCacheConfiguration(CacheConfiguration cfg) throws IgniteCheckedException;

    /**
     * Check whether object is of type {@code IgfsBlockKey}
     *
     * @param key Key.
     * @return {@code True} if IGFS block key.
     */
    public abstract boolean isIgfsBlockKey(Object key);
}