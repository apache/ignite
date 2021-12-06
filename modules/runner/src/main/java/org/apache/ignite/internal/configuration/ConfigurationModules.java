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

import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.configuration.annotation.ConfigurationType;

/**
 * An ensemble of {@link ConfigurationModule}s used to easily pick node-local and cluster-wide configuration
 * and merge modules for convenience.
 */
public class ConfigurationModules {
    private final CompoundModule localCompound;
    private final CompoundModule distributedCompound;

    /**
     * Creates a new instance of {@link ConfigurationModules} wrapping modules passed to it.
     *
     * @param modules modules to wrap; they may be of different types
     */
    public ConfigurationModules(List<ConfigurationModule> modules) {
        localCompound = compoundOfType(ConfigurationType.LOCAL, modules);
        distributedCompound = compoundOfType(ConfigurationType.DISTRIBUTED, modules);
    }

    /**
     * Return a module representing the result of merge of modules of type {@link ConfigurationType#LOCAL}.
     *
     * @return node-local configuration merge result
     */
    public ConfigurationModule local() {
        return localCompound;
    }

    /**
     * Return a module representing the result of merge of modules of type {@link ConfigurationType#DISTRIBUTED}.
     *
     * @return cluster-wide configuration merge result
     */
    public ConfigurationModule distributed() {
        return distributedCompound;
    }

    private static CompoundModule compoundOfType(ConfigurationType type, Collection<ConfigurationModule> modules) {
        List<ConfigurationModule> modulesOfGivenType = modules.stream()
                .filter(module -> module.type() == type)
                .collect(toUnmodifiableList());
        return new CompoundModule(type, modulesOfGivenType);
    }
}
