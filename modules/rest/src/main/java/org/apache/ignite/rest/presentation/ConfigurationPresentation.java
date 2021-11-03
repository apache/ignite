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

package org.apache.ignite.rest.presentation;

import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * Representing a configuration to/from representation form.
 *
 * @param <R> Representation form.
 */
public interface ConfigurationPresentation<R> {
    /**
     * Converts to presentation whole system configuration (all system modules are included).
     *
     * @return System configuration presentation converted to a given presentation type.
     */
    R represent();

    /**
     * Converts to presentation only a fraction of system configuration defined by given path.
     *
     * <p>If null path is passed method should fall back to returning whole system configuration.
     *
     * @param path Path to requested configuration in configuration tree or {@code null}.
     * @return Requested configuration fraction or whole configuration if {@code null} was passed.
     * @throws IllegalArgumentException If {@code path} is not found in current configuration.
     */
    R representByPath(@Nullable String path);

    /**
     * Converts and applies configuration update request to system configuration.
     *
     * @param cfgUpdate Configuration update request in representation form.
     * @throws IllegalArgumentException         If the configuration format is invalid.
     * @throws ConfigurationValidationException If configuration validation failed.
     * @throws IgniteException                  If an error happens.
     */
    void update(R cfgUpdate);
}
