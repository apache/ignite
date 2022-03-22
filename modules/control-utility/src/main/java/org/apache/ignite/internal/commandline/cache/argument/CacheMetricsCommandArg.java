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
import org.apache.ignite.internal.commandline.cache.CacheMetrics;
import org.apache.ignite.internal.visor.cache.metrics.CacheMetricsSubCommand;
import org.apache.ignite.internal.visor.cache.metrics.VisorCacheMetricsTaskArg;

/**
 * Command arguments for {@link CacheMetrics} command.
 */
public enum CacheMetricsCommandArg implements CommandArg {
    /** Enable. */
    ENABLE("--enable", CacheMetricsSubCommand.ENABLE),

    /** Disable. */
    DISABLE("--disable", CacheMetricsSubCommand.DISABLE),

    /** Status. */
    STATUS("--status", CacheMetricsSubCommand.STATUS),

    /** Perform command for all caches instead of defined list. */
    ALL_CACHES("--all-caches", null);

    /** Enable statistics flag. */
    private final String name;

    /** Sub-command value for {@link VisorCacheMetricsTaskArg}.*/
    private final CacheMetricsSubCommand taskArgSubCmd;

    /**
     * @param name Name.
     * @param taskArgSubCmd Sub-command value for {@link VisorCacheMetricsTaskArg}.
     */
    CacheMetricsCommandArg(String name, CacheMetricsSubCommand taskArgSubCmd) {
        this.name = name;
        this.taskArgSubCmd = taskArgSubCmd;
    }

    /** {@inheritDoc} */
    @Override public String argName() {
        return name;
    }

    /**
     * @return /** Sub-command value for {@link VisorCacheMetricsTaskArg}.
     */
    public CacheMetricsSubCommand taskArgumentSubCommand() {
        return taskArgSubCmd;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return name;
    }
}
