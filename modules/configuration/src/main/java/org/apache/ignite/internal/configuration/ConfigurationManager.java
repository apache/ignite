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

package org.apache.ignite.internal.configuration;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;

/**
 * Configuration manager is responsible for handling configuration lifecycle and provides configuration API.
 */
public class ConfigurationManager {
    /** Configuration registry. */
    private final ConfigurationRegistry confRegistry;

    /** Type mapped to configuration storage. */
    private final Map<ConfigurationType, ConfigurationStorage> configurationStorages;

    /**
     * The constructor.
     *
     * @param rootKeys Configuration root keys.
     * @param validators Validators.
     * @param configurationStorages Configuration storages.
     */
    public ConfigurationManager(
        Collection<RootKey<?, ?>> rootKeys,
        Map<Class<? extends Annotation>, Set<Validator<? extends Annotation, ?>>> validators,
        Collection<ConfigurationStorage> configurationStorages
    ) {
        HashMap<ConfigurationType, ConfigurationStorage> storageByType = new HashMap<>();

        for (ConfigurationStorage storage : configurationStorages) {
            assert !storageByType.containsKey(storage.type()) : "Two or more storage have the same configuration type [type=" + storage.type() + ']';

            storageByType.put(storage.type(), storage);
        }

        this.configurationStorages = Map.copyOf(storageByType);

        confRegistry = new ConfigurationRegistry(rootKeys, validators, configurationStorages);
    }

    /**
     * Constructor.
     *
     * @param rootKeys Configuration root keys.
     * @param configurationStorages Configuration storages.
     */
    public ConfigurationManager(
        Collection<RootKey<?, ?>> rootKeys,
        Collection<ConfigurationStorage> configurationStorages
    ) {
        this(rootKeys, Collections.emptyMap(), configurationStorages);
    }

    /**
     * Bootstrap configuration manager with customer user cfg.
     *
     * @param hoconStr Customer configuration in hocon format.
     * @param type Configuration type.
     * @throws InterruptedException If thread is interrupted during bootstrap.
     * @throws ExecutionException If configuration update failed for some reason.
     */
    public void bootstrap(String hoconStr, ConfigurationType type) throws InterruptedException, ExecutionException {
        ConfigObject hoconCfg = ConfigFactory.parseString(hoconStr).root();

        confRegistry.change(HoconConverter.hoconSource(hoconCfg), configurationStorages.get(type)).get();
    }

    /**
     * @return Configuration registry.
     */
    public ConfigurationRegistry configurationRegistry() {
        return confRegistry;
    }
}
