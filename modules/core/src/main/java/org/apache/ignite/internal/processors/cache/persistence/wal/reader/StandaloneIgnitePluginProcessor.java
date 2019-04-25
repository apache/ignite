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

package org.apache.ignite.internal.processors.cache.persistence.wal.reader;

import java.util.Collections;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.plugin.IgnitePluginProcessor;
import org.apache.ignite.plugin.PluginProvider;

/**
 * No operation, empty plugin processor for creating WAL iterator without node start up
 */
class StandaloneIgnitePluginProcessor extends IgnitePluginProcessor {
    /**
     * @param ctx Kernal context.
     * @param cfg Ignite configuration.
     */
    StandaloneIgnitePluginProcessor(GridKernalContext ctx, IgniteConfiguration cfg) throws IgniteCheckedException {
        super(ctx, cfg, Collections.<PluginProvider>emptyList());
    }
}
