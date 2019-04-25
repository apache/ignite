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

package org.apache.ignite.internal.processors.platform;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.resource.GridSpringResourceContext;
import org.jetbrains.annotations.Nullable;

/**
 * Platform bootstrap. Responsible for starting Ignite node with non-Java platform.
 */
public interface PlatformBootstrap {
    /**
     * Start Ignite node.
     *
     * @param cfg Configuration.
     * @param springCtx Optional Spring resource context.
     * @param envPtr Environment pointer.
     * @return Platform processor.
     */
    public PlatformProcessor start(IgniteConfiguration cfg, @Nullable GridSpringResourceContext springCtx, long envPtr);

    /**
     * Init the bootstrap.
     *
     * @param dataPtr Optional pointer to additional data required for startup.
     */
    public void init(long dataPtr);
}