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

package org.apache.ignite.platform.plugin;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.platform.PlatformPluginExtension;
import org.apache.ignite.internal.processors.platform.PlatformTarget;

/**
 * Test plugin extension.
 */
public class PlatformTestPluginExtension implements PlatformPluginExtension {
    /** */
    private final IgniteEx ignite;

    /**
     * Ctor.
     *
     * @param ignite Ignite.
     */
    PlatformTestPluginExtension(IgniteEx ignite) {
        assert ignite != null;

        this.ignite = ignite;
    }

    /** {@inheritDoc} */
    @Override public int id() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget createTarget() {
        return new PlatformTestPluginTarget(ignite.context().platform().context(), null);
    }
}
