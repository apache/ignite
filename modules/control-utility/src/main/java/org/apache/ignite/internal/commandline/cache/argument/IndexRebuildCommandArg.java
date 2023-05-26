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

package org.apache.ignite.internal.commandline.cache.argument;

import org.apache.ignite.internal.commandline.argument.CommandArg;
import org.apache.ignite.internal.commandline.cache.CacheScan;

/**
 * Arguments for {@link CacheScan} command.
 */
public enum IndexRebuildCommandArg implements CommandArg {
    /** Node id. */
    NODE_ID("--node-id"),

    /** Target cache and index names. Format: cacheName[indexName],cacheName2,cacheName3[idx1,idx2]. */
    CACHE_NAMES_TARGET("--cache-names"),

    /** Target cache groups' names. */
    CACHE_GROUPS_TARGET("--group-names");

    /** Argument name. */
    private final String name;

    /**
     * @param name Argument name.
     */
    IndexRebuildCommandArg(String name) {
        this.name = name;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
