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

package org.apache.ignite.internal.processors.cache.persistence.wal.memtracker;

import org.apache.ignite.plugin.PluginConfiguration;

/**
 * PageMemory tracker plugin configuration.
 */
public class PageMemoryTrackerConfiguration implements PluginConfiguration {
    /** Plugin enabled. */
    private boolean enabled;

    /** Perform page memory check on each checkpoint. */
    private boolean checkPagesOnCheckpoint;

    /**
     * Gets enabled flag.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets enabled flag.
     *
     * @param enabled Enabled.
     * @return {@code this} for chaining.
     */
    public PageMemoryTrackerConfiguration setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * Perform page memory check on each checkpoint.
     */
    public boolean isCheckPagesOnCheckpoint() {
        return checkPagesOnCheckpoint;
    }

    /**
     * Perform page memory check on each checkpoint.
     *
     * @param checkPagesOnCheckpoint Check on checkpoint.
     * @return {@code this} for chaining.
     */
    public PageMemoryTrackerConfiguration setCheckPagesOnCheckpoint(boolean checkPagesOnCheckpoint) {
        this.checkPagesOnCheckpoint = checkPagesOnCheckpoint;

        return this;
    }
}
