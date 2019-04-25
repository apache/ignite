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

package org.apache.ignite.platform.cpp;

import org.apache.ignite.configuration.PlatformConfiguration;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Platform CPP configuration.
 */
public class PlatformCppConfiguration implements PlatformConfiguration {
    /**
     * Default constructor.
     */
    public PlatformCppConfiguration() {
        // No-op.
    }

    /**
     * Copying constructor.
     *
     * @param cfg Configuration to copy.
     */
    public PlatformCppConfiguration(PlatformConfiguration cfg) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(PlatformConfiguration.class, this);
    }
}