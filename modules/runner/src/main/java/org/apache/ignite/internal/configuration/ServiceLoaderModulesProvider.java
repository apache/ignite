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

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;

/**
 * Provides {@link ConfigurationModule}s using {@link ServiceLoader} mechanism.
 * Uses {@link ConfigurationModule} interface to query the ServiceLoader.
 *
 * @see ConfigurationModule
 */
public class ServiceLoaderModulesProvider {
    /**
     * Loads modules using {@link ServiceLoader} mechanism.
     *
     * @param classLoader the class loader to use
     * @return modules
     */
    public List<ConfigurationModule> modules(ClassLoader classLoader) {
        return ServiceLoader.load(ConfigurationModule.class, classLoader).stream()
                .map(Provider::get)
                .collect(toUnmodifiableList());
    }
}
