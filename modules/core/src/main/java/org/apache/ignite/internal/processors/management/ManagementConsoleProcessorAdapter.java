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

package org.apache.ignite.internal.processors.management;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

/**
 * No-op implementation of Management Console Agent, throws exception on usage attempt.
 */
public abstract class ManagementConsoleProcessorAdapter extends GridProcessorAdapter {
    /** Management configuration instance. */
    protected ManagementConfiguration cfg = new ManagementConfiguration();

    /**
     * @param ctx Kernal context.
     */
    protected ManagementConsoleProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Sets the configuration.
     *
     * @param cfg Management configuration instance.
     */
    public void configuration(ManagementConfiguration cfg) {
        this.cfg = cfg;
    }

    /**
     * Gets the configuration.
     *
     * @return Management configuration instance.
     */
    public ManagementConfiguration configuration() {
        return cfg;
    }
}
