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

package org.apache.ignite.ml.inference.plugin;

import org.apache.ignite.plugin.PluginConfiguration;

public class MLInferencePluginConfiguration implements PluginConfiguration {

    private boolean withModelStorage;

    private boolean withModelDescriptorStorage;

    private Integer modelStorageBackups;

    private Integer modelDescriptorStorageBackups;

    public boolean isWithModelStorage() {
        return withModelStorage;
    }

    public void setWithModelStorage(boolean withModelStorage) {
        this.withModelStorage = withModelStorage;
    }

    public boolean isWithModelDescriptorStorage() {
        return withModelDescriptorStorage;
    }

    public void setWithModelDescriptorStorage(boolean withModelDescriptorStorage) {
        this.withModelDescriptorStorage = withModelDescriptorStorage;
    }

    public Integer getModelStorageBackups() {
        return modelStorageBackups;
    }

    public void setModelStorageBackups(Integer modelStorageBackups) {
        this.modelStorageBackups = modelStorageBackups;
    }

    public Integer getModelDescriptorStorageBackups() {
        return modelDescriptorStorageBackups;
    }

    public void setModelDescriptorStorageBackups(Integer modelDescriptorStorageBackups) {
        this.modelDescriptorStorageBackups = modelDescriptorStorageBackups;
    }
}
