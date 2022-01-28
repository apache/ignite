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

package org.apache.ignite.configuration;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.schemas.runner.ClusterConfiguration;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.store.PageMemoryDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.RocksDbDataRegionConfigurationSchema;
import org.apache.ignite.configuration.schemas.store.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.PartialIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.SortedIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.configuration.ConfigurationModule;

/**
 * {@link ConfigurationModule} for cluster-wide configuration provided by ignite-api.
 */
public class CoreDistributedConfigurationModule implements ConfigurationModule {
    /** {@inheritDoc} */
    @Override
    public ConfigurationType type() {
        return ConfigurationType.DISTRIBUTED;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<RootKey<?, ?>> rootKeys() {
        return List.of(
                ClusterConfiguration.KEY,
                TablesConfiguration.KEY,
                DataStorageConfiguration.KEY
        );
    }

    /** {@inheritDoc} */
    @Override
    public Collection<Class<?>> polymorphicSchemaExtensions() {
        return List.of(
                HashIndexConfigurationSchema.class,
                SortedIndexConfigurationSchema.class,
                PartialIndexConfigurationSchema.class,
                RocksDbDataRegionConfigurationSchema.class,
                PageMemoryDataRegionConfigurationSchema.class,
                UnsafeMemoryAllocatorConfigurationSchema.class
        );
    }
}
