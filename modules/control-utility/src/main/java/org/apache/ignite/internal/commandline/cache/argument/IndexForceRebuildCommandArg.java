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
import org.apache.ignite.internal.commandline.cache.CacheIndexesForceRebuild;

/**
 * Arguments for {@link CacheIndexesForceRebuild} command.
 */
public enum IndexForceRebuildCommandArg implements CommandArg {
    /** */
    NODE_ID("--node-id"),

    /** */
    GROUP_NAMES("--group-names"),

    /** */
    CACHE_NAMES("--cache-names");

    /** Argument name. */
    private final String name;

    /**
     * @param name Argument name.
     */
    IndexForceRebuildCommandArg(String name) {
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
