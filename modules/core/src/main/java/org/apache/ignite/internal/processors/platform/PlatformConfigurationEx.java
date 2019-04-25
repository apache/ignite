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

import org.apache.ignite.internal.processors.platform.cache.PlatformCacheExtension;
import org.apache.ignite.internal.logger.platform.PlatformLogger;
import org.apache.ignite.internal.processors.platform.callback.PlatformCallbackGateway;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemoryManagerImpl;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Extended platform configuration.
 */
public interface PlatformConfigurationEx {
    /*
     * @return Native gateway.
     */
    public PlatformCallbackGateway gate();

    /**
     * @return Memory manager.
     */
    public PlatformMemoryManagerImpl memory();

    /**
     * @return Platform name.
     */
    public String platform();

    /**
     * @return Warnings to be displayed on grid start.
     */
    public Collection<String> warnings();

    /**
     * @return Platform logger.
     */
    public PlatformLogger logger();

    /**
     * @return Available cache extensions.
     */
    @Nullable public Collection<PlatformCacheExtension> cacheExtensions();
}
