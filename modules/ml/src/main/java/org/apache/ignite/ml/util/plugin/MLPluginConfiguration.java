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

package org.apache.ignite.ml.util.plugin;

import org.apache.ignite.plugin.PluginConfiguration;

/**
 * Configuration of ML plugin that defines which ML inference services should be start up on Ignite startup.
 */
public class MLPluginConfiguration implements PluginConfiguration {
    /** Model storage should be created on startup. */
    private boolean withMdlStorage;

    /** Model descriptor storage should be created on startup. */
    private boolean withMdlDescStorage;

    /** Number of backups in model storage cache. */
    private Integer mdlStorageBackups;

    /** Number of backups in model descriptor storage cache. */
    private Integer mdlDescStorageBackups;

    /** */
    public boolean isWithMdlStorage() {
        return withMdlStorage;
    }

    /** */
    public void setWithMdlStorage(boolean withMdlStorage) {
        this.withMdlStorage = withMdlStorage;
    }

    /** */
    public boolean isWithMdlDescStorage() {
        return withMdlDescStorage;
    }

    /** */
    public void setWithMdlDescStorage(boolean withMdlDescStorage) {
        this.withMdlDescStorage = withMdlDescStorage;
    }

    /** */
    public Integer getMdlStorageBackups() {
        return mdlStorageBackups;
    }

    /** */
    public void setMdlStorageBackups(Integer mdlStorageBackups) {
        this.mdlStorageBackups = mdlStorageBackups;
    }

    /** */
    public Integer getMdlDescStorageBackups() {
        return mdlDescStorageBackups;
    }

    /** */
    public void setMdlDescStorageBackups(Integer mdlDescStorageBackups) {
        this.mdlDescStorageBackups = mdlDescStorageBackups;
    }
}
