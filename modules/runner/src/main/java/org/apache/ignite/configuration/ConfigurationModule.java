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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.configuration.extended.LocalConfiguration;
import org.apache.ignite.rest.configuration.InMemoryConfigurationStorage;
import org.apache.ignite.rest.presentation.json.JsonConverter;

/**
 * Module is responsible for preparing configuration when module is started.
 *
 * Preparing configuration includes reading it from configuration file, parsing it and initializing
 * {@link ConfigurationRegistry} object.
 */
@SuppressWarnings("PMD.UnusedPrivateField")
public class ConfigurationModule {
    /** */
    private LocalConfiguration localConfigurator;

    /** */
    private final ConfigurationRegistry confRegistry = new ConfigurationRegistry();

    /**
     * @param jsonStr
     */
    public void bootstrap(String jsonStr) throws InterruptedException {
        confRegistry.registerRootKey(LocalConfiguration.KEY);

        InMemoryConfigurationStorage storage = new InMemoryConfigurationStorage();

        confRegistry.registerStorage(storage);

        JsonObject jsonCfg = JsonParser.parseString(jsonStr).getAsJsonObject();

        try {
            confRegistry.change(Collections.emptyList(), JsonConverter.jsonSource(jsonCfg), storage).get();
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }

        localConfigurator = confRegistry.getConfiguration(LocalConfiguration.KEY);
    }

    /** */
    public ConfigurationRegistry configurationRegistry() {
        return confRegistry;
    }
}
